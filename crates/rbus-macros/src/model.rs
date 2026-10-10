//! `#[derive(Model)]` and `#[derive(Row)]`. Both expand to impls of
//! `rbus::provider::Model` / `Row` that describe the struct's members and
//! dispatch on relative paths; `rbus` does the rest.

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::spanned::Spanned;
use syn::{Data, DeriveInput, Fields, GenericArgument, Ident, Path, PathArguments, Result, Type};

use crate::attrs::{FieldAttrs, Hook, ModelAttrs, check_path};

/// Which derive is expanding.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Derive {
    Model,
    Row,
}

enum Kind {
    /// A `Prop<T>` of a model, or a plain `T` of a row.
    Value {
        ty: Type,
        writable: bool,
        validate: Option<Path>,
    },
    Table {
        row: Type,
        add_row: Hook,
        remove_row: Hook,
    },
    Event,
    /// A nested model (in a model) or `#[rbus(object)]` (in a row).
    Object {
        ty: Type,
    },
}

struct Member {
    ident: Ident,
    /// The path segment (`Radio`), or the whole event name (`Alarm!`).
    seg: String,
    kind: Kind,
}

/// The last path segment of `ty` and its single generic argument, for
/// `Prop<T>`, `Table<R>` and `Event<T>`.
fn wrapper(ty: &Type) -> Option<(String, Option<Type>)> {
    let Type::Path(p) = ty else { return None };
    let last = p.path.segments.last()?;
    let arg = match &last.arguments {
        PathArguments::AngleBracketed(a) if a.args.len() == 1 => match &a.args[0] {
            GenericArgument::Type(t) => Some(t.clone()),
            _ => None,
        },
        _ => None,
    };
    Some((last.ident.to_string(), arg))
}

fn members(input: &DeriveInput, derive: Derive) -> Result<Vec<Member>> {
    let Data::Struct(data) = &input.data else {
        return Err(syn::Error::new(
            input.ident.span(),
            "rbus derives work on structs with named fields",
        ));
    };
    let Fields::Named(fields) = &data.fields else {
        return Err(syn::Error::new(
            data.fields.span(),
            "rbus derives work on structs with named fields",
        ));
    };
    let mut out = Vec::new();
    for field in &fields.named {
        let attrs = FieldAttrs::parse(&field.attrs)?;
        let ident = field.ident.clone().expect("named field");
        if attrs.skip.is_some() {
            attrs.only("a skipped field", &["skip"])?;
            continue;
        }
        let wrapped = wrapper(&field.ty);
        let (outer, inner) = match &wrapped {
            Some((o, i)) => (o.as_str(), i.clone()),
            None => ("", None),
        };
        let generic = |what: &str| {
            inner.clone().ok_or_else(|| {
                syn::Error::new(field.ty.span(), format!("`{what}` needs its type argument"))
            })
        };
        let seg = match outer {
            "Event" => attrs.event_name(&ident)?,
            _ => attrs.segment(&ident)?,
        };
        let kind = match (derive, outer) {
            (_, "Table") => {
                attrs.only("a table", &["add_row", "remove_row"])?;
                Kind::Table {
                    row: generic("Table<Row>")?,
                    add_row: attrs.add_row,
                    remove_row: attrs.remove_row,
                }
            }
            (_, "Event") => {
                attrs.only("an event", &[])?;
                Kind::Event
            }
            (Derive::Model, "Prop") => {
                attrs.only("a property", &["writable", "validate"])?;
                Kind::Value {
                    ty: generic("Prop<T>")?,
                    writable: attrs.writable.is_some(),
                    validate: attrs.validate,
                }
            }
            (Derive::Model, _) => {
                attrs.only(
                    "a nested model (a field that isn't a Prop, Table or Event; mark other \
                     fields `#[rbus(skip)]`)",
                    &[],
                )?;
                Kind::Object {
                    ty: field.ty.clone(),
                }
            }
            (Derive::Row, _) if attrs.object.is_some() => {
                attrs.only("a sub-object", &["object"])?;
                Kind::Object {
                    ty: field.ty.clone(),
                }
            }
            (Derive::Row, "Prop") => {
                return Err(syn::Error::new(
                    field.ty.span(),
                    "row fields are plain values; the table publishes their changes, so use \
                     the value type itself instead of `Prop`",
                ));
            }
            (Derive::Row, _) => {
                attrs.only("a value", &["writable", "validate"])?;
                Kind::Value {
                    ty: field.ty.clone(),
                    writable: attrs.writable.is_some(),
                    validate: attrs.validate,
                }
            }
        };
        if let Kind::Value {
            writable: false,
            validate: Some(v),
            ..
        } = &kind
        {
            return Err(syn::Error::new(
                v.span(),
                "`validate` checks consumers' sets; add `writable` too",
            ));
        }
        out.push(Member { ident, seg, kind });
    }
    let mut seen = std::collections::HashSet::new();
    for m in &out {
        if !seen.insert(m.seg.clone()) {
            return Err(syn::Error::new(
                m.ident.span(),
                format!("two members are named `{}`", m.seg),
            ));
        }
    }
    Ok(out)
}

/// The code shared by both derives.
struct Gen {
    describe: Vec<TokenStream>,
    get: Vec<TokenStream>,
    set: Vec<TokenStream>,
    add_row: Vec<TokenStream>,
    remove_row: Vec<TokenStream>,
    bind: Vec<TokenStream>,
    /// Expressions true when a member is in use by a registered row.
    bound: Vec<TokenStream>,
    diff: Vec<TokenStream>,
}

fn generate(members: &[Member], derive: Derive) -> Gen {
    let p = quote!(::rbus::provider);
    let mut g = Gen {
        describe: Vec::new(),
        get: Vec::new(),
        set: Vec::new(),
        add_row: Vec::new(),
        remove_row: Vec::new(),
        bind: Vec::new(),
        bound: Vec::new(),
        diff: Vec::new(),
    };
    // Rows are borrowed mutably to set them; models are not.
    let nested_trait = match derive {
        Derive::Model => quote!(#p::Model),
        Derive::Row => quote!(#p::Row),
    };
    for m in members {
        let ident = &m.ident;
        let seg = &m.seg;
        match &m.kind {
            Kind::Value {
                ty,
                writable,
                validate,
            } => {
                let access = if *writable {
                    quote!(#p::Access::ReadWrite)
                } else {
                    quote!(#p::Access::ReadOnly)
                };
                g.describe.push(quote! {
                    let schema = schema.property(::std::format!("{}{}", prefix, #seg), #access);
                });
                let (read, store) = match derive {
                    Derive::Model => (
                        quote!(::std::result::Result::Ok(self.#ident.value())),
                        quote!(self.#ident.store(value)),
                    ),
                    Derive::Row => (
                        quote!(::std::result::Result::Ok(::rbus::IntoValue::into_value(
                            ::std::clone::Clone::clone(&self.#ident)
                        ))),
                        quote!(self.#ident = value),
                    ),
                };
                g.get.push(quote!([#seg] => #read,));
                if *writable {
                    let check = validate.as_ref().map(|v| quote!(#v(&value)?;));
                    g.set.push(quote! {
                        [#seg] => {
                            let value: #ty = #p::value_as(value)?;
                            #check
                            if apply {
                                #store;
                            }
                            ::std::result::Result::Ok(())
                        }
                    });
                } else {
                    g.set.push(quote! {
                        [#seg] => ::std::result::Result::Err(#p::Fault::not_writable()),
                    });
                }
                if derive == Derive::Row {
                    g.diff.push(quote! {
                        if self.#ident != old.#ident {
                            changes.push((
                                ::std::format!("{}{}", prefix, #seg),
                                ::rbus::IntoValue::into_value(::std::clone::Clone::clone(&old.#ident)),
                                ::rbus::IntoValue::into_value(::std::clone::Clone::clone(&self.#ident)),
                            ));
                        }
                    });
                } else {
                    g.bind.push(quote! {
                        self.#ident.bind(::std::format!("{}{}", prefix, #seg), handle);
                    });
                }
            }
            Kind::Table {
                row,
                add_row,
                remove_row,
            } => {
                let rows = if add_row.present() || remove_row.present() {
                    quote!(#p::Rows::Dynamic)
                } else {
                    quote!(#p::Rows::Fixed)
                };
                g.describe.push(quote! {
                    let schema = schema.table(::std::format!("{}{}.{{i}}.", prefix, #seg), #rows);
                    let schema = <#row as #p::Row>::describe(
                        schema,
                        &::std::format!("{}{}.{{i}}.", prefix, #seg),
                    );
                });
                g.get
                    .push(quote!([#seg, rest @ ..] => self.#ident.get_path(rest),));
                g.set
                    .push(quote!([#seg, rest @ ..] => self.#ident.set_path(rest, value, apply),));
                let make = match add_row {
                    Hook::Absent => None,
                    Hook::Default(_) => Some(quote!(<#row as ::std::default::Default>::default())),
                    Hook::Fn(f) => Some(quote!(#f(self, cx, alias).await?)),
                };
                if let Some(make) = make {
                    g.add_row.push(quote! {
                        [#seg] => {
                            let row: #row = #make;
                            self.#ident.insert_for(alias, row)
                        }
                    });
                }
                g.add_row.push(quote! {
                    [#seg, rest @ ..] if !rest.is_empty() => self.#ident.add_row_below(cx, rest, alias).await,
                });
                let before = match remove_row {
                    Hook::Absent => None,
                    Hook::Default(_) => Some(quote!()),
                    Hook::Fn(f) => Some(quote!(#f(self, cx, row).await?;)),
                };
                if let Some(before) = before {
                    g.remove_row.push(quote! {
                        [#seg] => {
                            #before
                            self.#ident.remove_for(row)
                        }
                    });
                }
                g.remove_row.push(quote! {
                    [#seg, rest @ ..] if !rest.is_empty() => self.#ident.remove_row_below(cx, rest, row).await,
                });
                g.bind.push(quote! {
                    self.#ident.bind(&::std::format!("{}{}.", prefix, #seg), handle);
                });
                g.bound.push(quote!(self.#ident.is_bound()));
            }
            Kind::Event => {
                g.describe.push(quote! {
                    let schema = schema.event(::std::format!("{}{}", prefix, #seg));
                });
                g.bind.push(quote! {
                    self.#ident.bind(::std::format!("{}{}", prefix, #seg), handle);
                });
                g.bound.push(quote!(self.#ident.is_bound()));
            }
            Kind::Object { ty } => {
                g.describe.push(quote! {
                    let schema = <#ty as #nested_trait>::describe(
                        schema,
                        &::std::format!("{}{}.", prefix, #seg),
                    );
                });
                let set_self = match derive {
                    Derive::Model => quote!(&self.#ident),
                    Derive::Row => quote!(&mut self.#ident),
                };
                g.get
                    .push(quote!([#seg, rest @ ..] => #nested_trait::get(&self.#ident, rest),));
                g.set.push(
                    quote!([#seg, rest @ ..] => #nested_trait::set(#set_self, rest, value, apply),),
                );
                g.add_row.push(quote! {
                    [#seg, rest @ ..] => #nested_trait::add_row(&self.#ident, cx, rest, alias).await,
                });
                g.remove_row.push(quote! {
                    [#seg, rest @ ..] => #nested_trait::remove_row(&self.#ident, cx, rest, row).await,
                });
                g.bind.push(quote! {
                    #nested_trait::bind(&self.#ident, &::std::format!("{}{}.", prefix, #seg), handle);
                });
                g.bound.push(quote!(#nested_trait::is_bound(&self.#ident)));
                if derive == Derive::Row {
                    g.diff.push(quote! {
                        #p::Row::diff(
                            &self.#ident,
                            &old.#ident,
                            &::std::format!("{}{}.", prefix, #seg),
                            changes,
                        );
                    });
                }
            }
        }
    }
    g
}

pub(crate) fn derive(input: DeriveInput, derive: Derive) -> Result<TokenStream> {
    let model_attrs = match derive {
        Derive::Model => Some(ModelAttrs::parse(&input.attrs)?),
        Derive::Row => {
            if let Some(a) = input.attrs.iter().find(|a| crate::attrs::is_rbus(a)) {
                return Err(syn::Error::new_spanned(
                    a,
                    "a row takes no struct options; its table's field in the model or outer \
                     row names it",
                ));
            }
            None
        }
    };
    let members = members(&input, derive)?;
    let Gen {
        describe,
        get,
        set,
        add_row,
        remove_row,
        bind,
        bound,
        diff,
    } = generate(&members, derive);
    let p = quote!(::rbus::provider);
    let ident = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
    let (trait_name, self_set) = match derive {
        Derive::Model => (quote!(#p::Model), quote!(&self)),
        Derive::Row => (quote!(#p::Row), quote!(&mut self)),
    };
    let not_found = quote!(_ => ::std::result::Result::Err(#p::Fault::not_found()),);
    let not_allowed = quote!(_ => ::std::result::Result::Err(#p::Fault::not_allowed()),);
    let rows = quote! {
        #[allow(unused_variables)]
        fn add_row<'rbus>(
            &'rbus self,
            cx: &'rbus #p::Context,
            path: &'rbus [&'rbus str],
            alias: ::std::option::Option<&'rbus str>,
        ) -> #p::BoxFuture<'rbus, ::std::result::Result<u32, #p::Fault>> {
            ::std::boxed::Box::pin(async move {
                match path {
                    #(#add_row)*
                    #not_allowed
                }
            })
        }

        #[allow(unused_variables)]
        fn remove_row<'rbus>(
            &'rbus self,
            cx: &'rbus #p::Context,
            path: &'rbus [&'rbus str],
            row: &'rbus ::rbus::RowId,
        ) -> #p::BoxFuture<'rbus, ::std::result::Result<(), #p::Fault>> {
            ::std::boxed::Box::pin(async move {
                match path {
                    #(#remove_row)*
                    #not_allowed
                }
            })
        }

        #[allow(unused_variables)]
        fn bind(&self, prefix: &str, handle: ::std::option::Option<&#p::Handle>) {
            #(#bind)*
        }
    };
    let extra = match derive {
        Derive::Model => quote!(),
        Derive::Row => quote! {
            #[allow(unused_variables)]
            fn diff(
                &self,
                old: &Self,
                prefix: &str,
                changes: &mut ::std::vec::Vec<(::std::string::String, ::rbus::Value, ::rbus::Value)>,
            ) {
                #(#diff)*
            }

            fn is_bound(&self) -> bool {
                false #(|| #bound)*
            }
        },
    };
    let mut out = quote! {
        impl #impl_generics #trait_name for #ident #ty_generics #where_clause {
            fn describe(schema: #p::Schema, prefix: &str) -> #p::Schema {
                #(#describe)*
                schema
            }

            fn get(&self, path: &[&str]) -> ::std::result::Result<::rbus::Value, #p::Fault> {
                match path {
                    #(#get)*
                    #not_found
                }
            }

            #[allow(unused_variables)]
            fn set(
                #self_set,
                path: &[&str],
                value: &::rbus::Value,
                apply: bool,
            ) -> ::std::result::Result<(), #p::Fault> {
                match path {
                    #(#set)*
                    #not_found
                }
            }

            #rows
            #extra
        }
    };
    if let Some(attrs) = model_attrs {
        if let Some(path) = &attrs.path {
            out.extend(provider_impl(&input, &attrs, &check_path(path, false)?));
        }
    }
    Ok(out)
}

/// `impl Provider` for a model with a path.
fn provider_impl(input: &DeriveInput, attrs: &ModelAttrs, path: &str) -> TokenStream {
    let p = quote!(::rbus::provider);
    let ident = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
    let serialized = attrs
        .serialized
        .then(|| quote!(let schema = schema.serialized();));
    let commit = attrs
        .on_commit
        .as_ref()
        .map(|f| quote!(#f(self, cx, &tx).await?;));
    let on_subscribe = attrs.on_subscribe.as_ref().map(|f| {
        quote! {
            async fn on_subscribe(
                &self,
                cx: &#p::Context,
                subscriber: &#p::SubscriberInfo,
            ) -> ::std::result::Result<(), #p::Fault> {
                #f(self, cx, subscriber).await
            }
        }
    });
    let path_const = format_ident!("__RBUS_PATH");
    quote! {
        impl #impl_generics #p::Provider for #ident #ty_generics #where_clause {
            fn schema(&self) -> #p::Schema {
                let schema = <Self as #p::Model>::describe(#p::Schema::new(), #path);
                #serialized
                schema
            }

            async fn get(
                &self,
                _cx: &#p::Context,
                name: &::rbus::PropertyName,
            ) -> ::std::result::Result<::rbus::Value, #p::Fault> {
                #p::Model::get_at(self, #path, name)
            }

            #[allow(unused_variables)]
            async fn set(
                &self,
                cx: &#p::Context,
                tx: #p::Transaction,
            ) -> ::std::result::Result<(), #p::SetFault> {
                const #path_const: &str = #path;
                #p::Model::check_transaction(self, #path_const, &tx)?;
                #commit
                #p::Model::apply_transaction(self, #path_const, &tx);
                ::std::result::Result::Ok(())
            }

            async fn add_row(
                &self,
                cx: &#p::Context,
                table: &::rbus::TablePath,
                alias: ::std::option::Option<&str>,
            ) -> ::std::result::Result<u32, #p::Fault> {
                #p::Model::add_row_at(self, #path, cx, table, alias).await
            }

            async fn remove_row(
                &self,
                cx: &#p::Context,
                table: &::rbus::TablePath,
                row: &::rbus::RowId,
            ) -> ::std::result::Result<(), #p::Fault> {
                #p::Model::remove_row_at(self, #path, cx, table, row).await
            }

            #on_subscribe

            fn attach(&self, handle: #p::Handle) {
                #p::Model::bind(self, #path, ::std::option::Option::Some(&handle));
            }
        }
    }
}
