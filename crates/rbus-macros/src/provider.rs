//! `#[rbus::provider]` on an impl block: computed values and methods.

use proc_macro2::{Span, TokenStream};
use quote::quote;
use syn::spanned::Spanned;
use syn::{FnArg, Ident, ImplItem, ImplItemFn, ItemImpl, LitStr, Result, Type};

use crate::attrs::{check_path, is_rbus};
use crate::names::{self, Kind};

/// The impl block's options: `path = "..."` and `serialized`.
pub(crate) struct ProviderArgs {
    path: LitStr,
    serialized: bool,
    /// `crate = "path"`: where `rbus` is, if not `::rbus`.
    krate: Option<syn::Path>,
}

impl ProviderArgs {
    pub(crate) fn parse(args: TokenStream) -> Result<Self> {
        let mut path = None;
        let mut serialized = false;
        let mut krate = None;
        let parser = syn::meta::parser(|meta| {
            if meta.path.is_ident("path") {
                path = Some(meta.value()?.parse()?);
            } else if meta.path.is_ident("serialized") {
                serialized = true;
            } else if meta.path.is_ident("crate") {
                krate = Some(crate::attrs::crate_path(&meta)?);
            } else {
                return Err(meta.error(
                    "unknown rbus::provider option; expected `path`, `serialized` or `crate`",
                ));
            }
            Ok(())
        });
        syn::parse::Parser::parse2(parser, args)?;
        let path = path.ok_or_else(|| {
            syn::Error::new(
                Span::call_site(),
                "`#[rbus::provider]` needs a path: #[rbus::provider(path = \"Device.Example.\")]",
            )
        })?;
        Ok(ProviderArgs {
            path,
            serialized,
            krate,
        })
    }
}

/// What one annotated function provides.
enum Role {
    Get { name: String, poll: Option<u64> },
    Set { name: String, ty: Type },
    Method { name: String, args: Option<Type> },
}

struct Handler {
    ident: Ident,
    asyncness: bool,
    role: Role,
}

/// The value of a `get = "..."` style option, if there is one.
fn optional_name(meta: &syn::meta::ParseNestedMeta<'_>) -> Result<Option<LitStr>> {
    if meta.input.peek(syn::Token![=]) {
        Ok(Some(meta.value()?.parse()?))
    } else {
        Ok(None)
    }
}

/// `"2s"`, `"500ms"` or `"1m"` in milliseconds.
fn parse_period(lit: &LitStr) -> Result<u64> {
    let s = lit.value();
    let (digits, unit) = s.split_at(s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len()));
    let n: u64 = digits.parse().map_err(|_| {
        syn::Error::new(
            lit.span(),
            "a period is a number and a unit: \"500ms\", \"2s\" or \"1m\"",
        )
    })?;
    let ms = match unit {
        "ms" => n,
        "s" => n * 1000,
        "m" => n * 60_000,
        _ => {
            return Err(syn::Error::new(
                lit.span(),
                "a period's unit is ms, s or m: \"500ms\", \"2s\" or \"1m\"",
            ));
        }
    };
    if ms == 0 {
        return Err(syn::Error::new(lit.span(), "a period can't be zero"));
    }
    Ok(ms)
}

/// The type of the function's `n`th argument after `&self`.
fn arg_type(f: &ImplItemFn, n: usize) -> Option<Type> {
    match f.sig.inputs.iter().nth(n + 1)? {
        FnArg::Typed(t) => Some((*t.ty).clone()),
        FnArg::Receiver(_) => None,
    }
}

fn check_leaf(lit: &LitStr, kind: Kind) -> Result<String> {
    let name = lit.value();
    let base = match kind {
        Kind::Method => name.strip_suffix("()").unwrap_or(&name),
        _ => &name,
    };
    names::check_segment(base).map_err(|e| syn::Error::new(lit.span(), e))?;
    Ok(match kind {
        Kind::Method => format!("{base}()"),
        _ => name,
    })
}

/// The impl block with every `#[rbus(..)]` attribute removed.
pub(crate) fn without_rbus_attrs(mut item: ItemImpl) -> ItemImpl {
    for it in &mut item.items {
        if let ImplItem::Fn(f) = it {
            f.attrs.retain(|a| !is_rbus(a));
        }
    }
    item
}

/// Parses the `#[rbus(..)]` options of one function and removes them.
fn handler(f: &mut ImplItemFn) -> Result<Option<Handler>> {
    let attrs: Vec<_> = f.attrs.iter().filter(|a| is_rbus(a)).cloned().collect();
    f.attrs.retain(|a| !is_rbus(a));
    if attrs.is_empty() {
        return Ok(None);
    }
    let mut get = None;
    let mut set = None;
    let mut method = None;
    let mut poll = None;
    for attr in &attrs {
        attr.parse_nested_meta(|meta| {
            let p = &meta.path;
            if p.is_ident("get") {
                get = Some((optional_name(&meta)?, p.span()));
            } else if p.is_ident("set") {
                set = Some((optional_name(&meta)?, p.span()));
            } else if p.is_ident("method") {
                method = Some((optional_name(&meta)?, p.span()));
            } else if p.is_ident("poll") {
                let lit: LitStr = meta.value()?.parse()?;
                poll = Some((parse_period(&lit)?, lit.span()));
            } else {
                return Err(meta.error("unknown rbus option; expected get, set, method or poll"));
            }
            Ok(())
        })?;
    }
    let ident = f.sig.ident.clone();
    let fn_name = ident.to_string();
    let roles = get.is_some() as u8 + set.is_some() as u8 + method.is_some() as u8;
    if roles != 1 {
        return Err(syn::Error::new_spanned(
            &attrs[0],
            "each function is one of `get`, `set` or `method`",
        ));
    }
    if let (Some((_, span)), None) = (&poll, &get) {
        return Err(syn::Error::new(*span, "`poll` goes with `get`"));
    }
    if !matches!(f.sig.inputs.first(), Some(FnArg::Receiver(_))) {
        return Err(syn::Error::new(
            f.sig.span(),
            "a provider function takes `&self` and `cx: &Context` first",
        ));
    }
    let role = if let Some((lit, _)) = get {
        let name = match lit {
            Some(l) => check_leaf(&l, Kind::Property)?,
            None => names::pascal_case(&fn_name),
        };
        Role::Get {
            name,
            poll: poll.map(|(ms, _)| ms),
        }
    } else if let Some((lit, span)) = set {
        let name = match lit {
            Some(l) => check_leaf(&l, Kind::Property)?,
            None => names::pascal_case(fn_name.strip_prefix("set_").unwrap_or(&fn_name)),
        };
        let ty = arg_type(f, 1).ok_or_else(|| {
            syn::Error::new(
                span,
                "a setter takes `&self`, `cx: &Context` and the new value",
            )
        })?;
        Role::Set { name, ty }
    } else {
        let (lit, _) = method.expect("one role");
        let name = match lit {
            Some(l) => check_leaf(&l, Kind::Method)?,
            None => format!("{}()", names::pascal_case(&fn_name)),
        };
        Role::Method {
            name,
            args: arg_type(f, 1),
        }
    };
    Ok(Some(Handler {
        ident,
        asyncness: f.sig.asyncness.is_some(),
        role,
    }))
}

/// One property: its getter, setter and polling period.
#[derive(Default)]
struct Property {
    get: Option<(Ident, bool)>,
    set: Option<(Ident, bool, Type)>,
    poll: Option<u64>,
}

pub(crate) fn expand(args: ProviderArgs, mut item: ItemImpl) -> Result<TokenStream> {
    let path = check_path(&args.path, true)?;
    let krate = args
        .krate
        .as_ref()
        .map_or_else(|| quote!(::rbus), |p| quote!(#p));
    if let Some((_, trait_path, _)) = &item.trait_ {
        return Err(syn::Error::new(
            trait_path.span(),
            "`#[rbus::provider]` goes on an inherent impl block; it implements `Provider` itself",
        ));
    }
    let mut handlers = Vec::new();
    for it in &mut item.items {
        if let ImplItem::Fn(f) = it {
            if let Some(h) = handler(f)? {
                handlers.push((h, f.sig.ident.span()));
            }
        }
    }
    if handlers.is_empty() {
        return Err(syn::Error::new(
            item.self_ty.span(),
            "no `#[rbus(get)]`, `#[rbus(set)]` or `#[rbus(method)]` functions to serve",
        ));
    }
    // Properties in order of first appearance, then methods.
    let mut properties: Vec<(String, Property)> = Vec::new();
    let mut methods: Vec<(String, Ident, bool, Option<Type>)> = Vec::new();
    for (h, span) in handlers {
        let dup = |name: &str| {
            syn::Error::new(
                span,
                format!("`{name}` has two functions for the same role"),
            )
        };
        match h.role {
            Role::Get { name, poll } => {
                let prop = property(&mut properties, &name);
                if prop.get.is_some() {
                    return Err(dup(&name));
                }
                prop.get = Some((h.ident, h.asyncness));
                prop.poll = poll;
            }
            Role::Set { name, ty } => {
                let prop = property(&mut properties, &name);
                if prop.set.is_some() {
                    return Err(dup(&name));
                }
                prop.set = Some((h.ident, h.asyncness, ty));
            }
            Role::Method { name, args } => {
                if methods.iter().any(|m| m.0 == name) {
                    return Err(dup(&name));
                }
                methods.push((name, h.ident, h.asyncness, args));
            }
        }
    }
    let p = quote!(#krate::provider);
    let awaited = |ident: &Ident, asyncness: bool, args: TokenStream| {
        let call = quote!(Self::#ident(self, cx, #args));
        if asyncness { quote!(#call.await) } else { call }
    };
    let mut schema = Vec::new();
    let mut gets = Vec::new();
    let mut checks = Vec::new();
    let mut sets = Vec::new();
    for (name, prop) in &properties {
        let full = format!("{path}{name}");
        let access = match (&prop.get, &prop.set) {
            (Some(_), Some(_)) => quote!(#p::Access::ReadWrite),
            (Some(_), None) => quote!(#p::Access::ReadOnly),
            _ => quote!(#p::Access::WriteOnly),
        };
        let poll = prop
            .poll
            .map(|ms| quote!(let schema = schema.poll(::std::time::Duration::from_millis(#ms));));
        schema.push(quote! {
            let schema = schema.property(#full, #access);
            #poll
        });
        if let Some((ident, asyncness)) = &prop.get {
            let call = awaited(ident, *asyncness, quote!());
            gets.push(quote! {
                #name => #call
                    .map(#krate::IntoValue::into_value)
                    .map_err(::std::convert::Into::<#p::Fault>::into),
            });
        }
        if let Some((ident, asyncness, ty)) = &prop.set {
            let call = awaited(ident, *asyncness, quote!(change.get::<#ty>()?));
            checks.push(quote!(#name => { change.get::<#ty>()?; }));
            sets.push(quote! {
                #name => #call
                    .map_err(|f| #p::SetFault::at(change.name(), f))?,
            });
        }
    }
    let mut invokes = Vec::new();
    for (name, ident, asyncness, args) in &methods {
        let full = format!("{path}{name}");
        schema.push(quote!(let schema = schema.method(#full);));
        let (decode, pass) = match args {
            Some(ty) => (
                quote! {
                    let args: #ty = #krate::from_object(args)
                        .map_err(|e| #p::Fault::invalid_input(::std::string::ToString::to_string(&e)))?;
                },
                quote!(args),
            ),
            None => (quote!(let _ = args;), quote!()),
        };
        let call = awaited(ident, *asyncness, pass);
        invokes.push(quote! {
            #name => {
                #decode
                let reply = #call.map_err(::std::convert::Into::<#p::Fault>::into)?;
                #krate::to_object(&reply).map_err(#p::Fault::internal)
            }
        });
    }
    let leaf = quote!(name.rsplit('.').next().unwrap_or_default());
    let serialized = args
        .serialized
        .then(|| quote!(let schema = schema.serialized();));
    let set_fn = (!sets.is_empty()).then(|| {
        quote! {
            async fn set(
                &self,
                cx: &#p::Context,
                tx: #p::Transaction,
            ) -> ::std::result::Result<(), #p::SetFault> {
                // Every value converts before any setter runs.
                for change in &tx {
                    let name = change.name().as_str();
                    match #leaf {
                        #(#checks)*
                        _ => return ::std::result::Result::Err(
                            #p::SetFault::at(change.name(), #p::Fault::not_writable()),
                        ),
                    }
                }
                for change in &tx {
                    let name = change.name().as_str();
                    match #leaf {
                        #(#sets)*
                        _ => {}
                    }
                }
                ::std::result::Result::Ok(())
            }
        }
    });
    let invoke_fn = (!invokes.is_empty()).then(|| {
        quote! {
            async fn invoke(
                &self,
                cx: &#p::Context,
                method: &#krate::MethodName,
                args: #krate::Object,
            ) -> ::std::result::Result<#krate::Object, #p::Fault> {
                let name = method.as_str();
                match #leaf {
                    #(#invokes)*
                    _ => ::std::result::Result::Err(#p::Fault::invalid_method()),
                }
            }
        }
    });
    let self_ty = &item.self_ty;
    let (impl_generics, _, where_clause) = item.generics.split_for_impl();
    Ok(quote! {
        #item

        impl #impl_generics #p::Provider for #self_ty #where_clause {
            fn schema(&self) -> #p::Schema {
                let schema = #p::Schema::new();
                #(#schema)*
                #serialized
                schema
            }

            #[allow(unused_variables)]
            async fn get(
                &self,
                cx: &#p::Context,
                name: &#krate::PropertyName,
            ) -> ::std::result::Result<#krate::Value, #p::Fault> {
                let name = name.as_str();
                match #leaf {
                    #(#gets)*
                    _ => ::std::result::Result::Err(#p::Fault::not_found()),
                }
            }

            #set_fn
            #invoke_fn
        }
    })
}

fn property<'a>(properties: &'a mut Vec<(String, Property)>, name: &str) -> &'a mut Property {
    let i = match properties.iter().position(|(n, _)| n == name) {
        Some(i) => i,
        None => {
            properties.push((name.to_owned(), Property::default()));
            properties.len() - 1
        }
    };
    &mut properties[i].1
}
