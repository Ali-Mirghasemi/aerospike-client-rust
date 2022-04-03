use syn::{self, Generics, ExprPath};
use syn::Ident;
use syn::Meta::{NameValue};
use syn::NestedMeta::{Lit, Meta};
use quote::ToTokens;
use super::symbol::{NAMESPACE_FN, SET_NAME_FN};
use super::{get_entity_meta_items, get_lit_str, parse_lit_into_expr_path};
use super::{context::Context, symbol::{NAMESPACE, SET_NAME}};

pub struct  Container {
    pub ident:          Ident,
    pub generics:       Generics,
    pub namespace:      String,
    pub namespace_fn:   Option<ExprPath>,
    pub set_name:       String,
    pub set_name_fn:    Option<ExprPath>,
}


impl Container {

    pub fn from_ast(ctx: &Context, item: &syn::DeriveInput) -> Self {
        let mut namespace = "test".to_owned();
        let mut set_name = "test".to_owned();
        let mut namespace_fn = None;
        let mut set_name_fn = None;

        for meta_item in item
            .attrs
            .iter()
            .flat_map(|attr| get_entity_meta_items(ctx, attr))
            .flatten()
        {
            match &meta_item {
                Meta(NameValue(m)) if m.path == NAMESPACE_FN => {
                    if let Ok(s) = parse_lit_into_expr_path(ctx, NAMESPACE, &m.lit) {
                        namespace_fn = Some(s);
                    }
                },
                Meta(NameValue(m)) if m.path == NAMESPACE => {
                    if let Ok(s) = get_lit_str(ctx, NAMESPACE, &m.lit) {
                        namespace = s.value();
                    }
                },
                Meta(NameValue(m)) if m.path == SET_NAME_FN => {
                    if let Ok(s) = parse_lit_into_expr_path(ctx, SET_NAME, &m.lit) {
                        set_name_fn = Some(s);
                    }
                },
                Meta(NameValue(m)) if m.path == SET_NAME => {
                    if let Ok(s) = get_lit_str(ctx, SET_NAME, &m.lit) {
                        set_name = s.value();
                    }
                },
                Meta(meta_item) => {
                    let path = meta_item
                        .path()
                        .into_token_stream()
                        .to_string()
                        .replace(' ', "");
                    ctx.error_spanned_by(
                        meta_item.path(),
                        format!("unknown entity container attribute `{}`", path),
                    );
                },
                Lit(lit) => {
                    ctx.error_spanned_by(lit, "unexpected literal in entity container attribute");
                }
            }
        }

        Container { 
            ident: item.ident.clone(),
            generics: item.generics.clone(),
            namespace,
            namespace_fn,
            set_name,
            set_name_fn,
        }
    }

}

