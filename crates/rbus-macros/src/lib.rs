//! Procedural macros for `rbus`. Every macro expands to impls of `rbus`
//! traits (`Provider`, `Model`, `Row`) that could be written by hand. Use
//! them through the `rbus` crate rather than depending on this crate
//! directly.
#![forbid(unsafe_code)]

mod attrs;
mod model;
mod names;
mod provider;

use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{DeriveInput, ItemImpl, LitStr, parse_macro_input};

/// A typed element name, checked while compiling.
///
/// The suffix picks the type: `()` makes a `MethodName`, `!` an `EventName`,
/// `.` a `TablePath`, and anything else a `PropertyName`. A malformed name is
/// a compile error.
///
/// ```ignore
/// const REBOOT: rbus::MethodName = rbus::name!("Device.Reboot()");
/// ```
#[proc_macro]
pub fn name(input: TokenStream) -> TokenStream {
    let lit = parse_macro_input!(input as LitStr);
    let name = lit.value();
    match names::check(&name) {
        Ok(kind) => {
            let ty = format_ident!("{}", kind.type_name());
            quote! {{
                const NAME: ::rbus::#ty = ::rbus::#ty::from_static(#lit);
                NAME
            }}
            .into()
        }
        Err(reason) => syn::Error::new(lit.span(), format!("invalid rbus name {name:?}: {reason}"))
            .to_compile_error()
            .into(),
    }
}

/// Serves a struct's state as a data model.
///
/// Fields are `Prop<T>` (a property), `Table<Row>` (a table), `Event<T>`
/// (an event) or another `Model` (a sub-object); mark anything else
/// `#[rbus(skip)]`. Names are the field names in PascalCase.
///
/// Struct options: `path = "Device.X."` (makes the struct a `Provider`),
/// `on_subscribe = fn`, `on_commit = fn`, `serialized`, and
/// `crate = "path"` when `rbus` is renamed in `Cargo.toml`.
///
/// Field options: `name = "Name"`, `writable`, `validate = fn` (properties);
/// `add_row`, `add_row = fn`, `remove_row`, `remove_row = fn` (tables);
/// `skip`.
#[proc_macro_derive(Model, attributes(rbus))]
pub fn derive_model(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    model::derive(input, model::Derive::Model)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// A table row for `Table<Row>`.
///
/// Fields are plain values (any `RbusType + Clone + PartialEq`), `Table`s
/// (nested tables), `Event`s, or `#[rbus(object)]` sub-objects whose type
/// also derives `Row`. Names are the field names in PascalCase.
///
/// Field options: `name = "Name"`, `writable`, `validate = fn` (values);
/// `add_row[ = fn]`, `remove_row[ = fn]` (tables); `object`; `skip`. The
/// only struct option is `crate = "path"`, as for `Model`.
#[proc_macro_derive(Row, attributes(rbus))]
pub fn derive_row(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    model::derive(input, model::Derive::Row)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// Serves an impl block's functions as properties and methods:
/// `#[rbus::provider(path = "Device.WiFi.Radio.{i}.")]`.
///
/// Function options: `get[ = "Name"]` and `poll = "2s"`, `set[ = "Name"]`,
/// `method[ = "Name()"]`. Names default to the function name in PascalCase
/// (without `set_` for setters). The impl block takes `serialized` and
/// `crate = "path"` (when `rbus` is renamed) next to `path`.
#[proc_macro_attribute]
pub fn provider(args: TokenStream, input: TokenStream) -> TokenStream {
    let item = parse_macro_input!(input as ItemImpl);
    provider::ProviderArgs::parse(args.into())
        .and_then(|args| provider::expand(args, item.clone()))
        .unwrap_or_else(|e| {
            // Keep the functions, so the error is the only one.
            let item = provider::without_rbus_attrs(item);
            let e = e.into_compile_error();
            quote!(#item #e)
        })
        .into()
}
