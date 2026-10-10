//! `#[rbus(...)]` attribute parsing.

use proc_macro2::Span;
use syn::spanned::Spanned;
use syn::{Attribute, LitStr, Path, Result};

use crate::names;

/// A hook a table field names: `add_row` alone (rows start as
/// `Default::default()` / removal needs no hook) or `add_row = path`.
#[derive(Default)]
pub(crate) enum Hook {
    #[default]
    Absent,
    Default(Span),
    Fn(Path),
}

impl Hook {
    pub(crate) fn present(&self) -> bool {
        !matches!(self, Hook::Absent)
    }

    fn span(&self) -> Option<Span> {
        match self {
            Hook::Absent => None,
            Hook::Default(s) => Some(*s),
            Hook::Fn(p) => Some(p.span()),
        }
    }
}

/// The options of a field, for `#[derive(Model)]` and `#[derive(Row)]`.
#[derive(Default)]
pub(crate) struct FieldAttrs {
    pub(crate) name: Option<LitStr>,
    pub(crate) writable: Option<Span>,
    pub(crate) validate: Option<Path>,
    pub(crate) add_row: Hook,
    pub(crate) remove_row: Hook,
    pub(crate) object: Option<Span>,
    pub(crate) skip: Option<Span>,
}

/// True for our own attributes.
pub(crate) fn is_rbus(attr: &Attribute) -> bool {
    attr.path().is_ident("rbus")
}

fn hook(meta: &syn::meta::ParseNestedMeta<'_>) -> Result<Hook> {
    if meta.input.peek(syn::Token![=]) {
        Ok(Hook::Fn(meta.value()?.parse()?))
    } else {
        Ok(Hook::Default(meta.path.span()))
    }
}

impl FieldAttrs {
    pub(crate) fn parse(attrs: &[Attribute]) -> Result<Self> {
        let mut out = FieldAttrs::default();
        for attr in attrs.iter().filter(|a| is_rbus(a)) {
            attr.parse_nested_meta(|meta| {
                let p = &meta.path;
                if p.is_ident("name") {
                    out.name = Some(meta.value()?.parse()?);
                } else if p.is_ident("writable") {
                    out.writable = Some(p.span());
                } else if p.is_ident("validate") {
                    out.validate = Some(meta.value()?.parse()?);
                } else if p.is_ident("add_row") {
                    out.add_row = hook(&meta)?;
                } else if p.is_ident("remove_row") {
                    out.remove_row = hook(&meta)?;
                } else if p.is_ident("object") {
                    out.object = Some(p.span());
                } else if p.is_ident("skip") {
                    out.skip = Some(p.span());
                } else {
                    return Err(meta.error(
                        "unknown rbus field option; expected one of: name, writable, validate, \
                         add_row, remove_row, object, skip",
                    ));
                }
                Ok(())
            })?;
        }
        Ok(out)
    }

    /// Fails if an option is set that a member of this kind doesn't take.
    pub(crate) fn only(&self, kind: &str, allowed: &[&str]) -> Result<()> {
        let set: [(&str, Option<Span>); 6] = [
            ("writable", self.writable),
            ("validate", self.validate.as_ref().map(Spanned::span)),
            ("add_row", self.add_row.span()),
            ("remove_row", self.remove_row.span()),
            ("object", self.object),
            ("skip", self.skip),
        ];
        for (option, span) in set {
            if let Some(span) = span {
                if !allowed.contains(&option) {
                    return Err(syn::Error::new(
                        span,
                        format!("`{option}` doesn't apply to {kind}"),
                    ));
                }
            }
        }
        Ok(())
    }

    /// The member's path segment: the `name` option, checked, or the field
    /// name in PascalCase.
    pub(crate) fn segment(&self, ident: &syn::Ident) -> Result<String> {
        match &self.name {
            Some(lit) => {
                let name = lit.value();
                names::check_segment(&name).map_err(|e| syn::Error::new(lit.span(), e))?;
                Ok(name)
            }
            None => Ok(names::pascal_case(&ident.to_string())),
        }
    }

    /// An event's name: the `name` option as written (C providers also
    /// register events without `!`), or the field name in PascalCase plus
    /// `!`.
    pub(crate) fn event_name(&self, ident: &syn::Ident) -> Result<String> {
        match &self.name {
            Some(lit) => {
                let name = lit.value();
                names::check_segment(name.strip_suffix('!').unwrap_or(&name))
                    .map_err(|e| syn::Error::new(lit.span(), e))?;
                Ok(name)
            }
            None => Ok(format!("{}!", names::pascal_case(&ident.to_string()))),
        }
    }
}

/// The options of a `#[derive(Model)]` struct.
#[derive(Default)]
pub(crate) struct ModelAttrs {
    pub(crate) path: Option<LitStr>,
    pub(crate) on_subscribe: Option<Path>,
    pub(crate) on_commit: Option<Path>,
    pub(crate) serialized: bool,
}

impl ModelAttrs {
    pub(crate) fn parse(attrs: &[Attribute]) -> Result<Self> {
        let mut out = ModelAttrs::default();
        for attr in attrs.iter().filter(|a| is_rbus(a)) {
            attr.parse_nested_meta(|meta| {
                let p = &meta.path;
                if p.is_ident("path") {
                    out.path = Some(meta.value()?.parse()?);
                } else if p.is_ident("on_subscribe") {
                    out.on_subscribe = Some(meta.value()?.parse()?);
                } else if p.is_ident("on_commit") {
                    out.on_commit = Some(meta.value()?.parse()?);
                } else if p.is_ident("serialized") {
                    out.serialized = true;
                } else {
                    return Err(meta.error(
                        "unknown rbus model option; expected one of: path, on_subscribe, \
                         on_commit, serialized",
                    ));
                }
                Ok(())
            })?;
        }
        if out.path.is_none() {
            let needs_path = [
                ("on_subscribe", out.on_subscribe.as_ref().map(Spanned::span)),
                ("on_commit", out.on_commit.as_ref().map(Spanned::span)),
            ];
            for (option, span) in needs_path {
                if let Some(span) = span {
                    return Err(syn::Error::new(
                        span,
                        format!("`{option}` needs `path`: only a model with a path is a provider"),
                    ));
                }
            }
        }
        Ok(out)
    }
}

/// Checks a model's or impl block's `path`: an object path ending in `.`.
/// `templates` allows `{i}` (impl blocks under a table).
pub(crate) fn check_path(lit: &LitStr, templates: bool) -> Result<String> {
    let path = lit.value();
    let err = |msg: &str| syn::Error::new(lit.span(), msg);
    if !path.ends_with('.') {
        return Err(err("the path must end in '.', like \"Device.WiFi.\""));
    }
    if path.contains("{i}") && !templates {
        return Err(err(
            "a model's path can't contain `{i}`; put the model's rows in a `Table` instead",
        ));
    }
    let plain = path.replace("{i}", "0");
    if let Err(e) = names::check(&plain) {
        return Err(err(&format!("invalid path: {e}")));
    }
    if plain.contains(['*', '[', ']', '{', '}']) {
        return Err(err(
            "the path can't contain wildcards or aliases; `{i}` marks a table row",
        ));
    }
    Ok(path)
}
