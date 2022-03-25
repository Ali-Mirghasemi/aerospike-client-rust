use syn::{self, Generics};
use syn::parse::{self, Parse, ParseStream};
use syn::punctuated::Punctuated;
use syn::Ident;
use syn::Meta::{List, NameValue, Path};
use syn::NestedMeta::{Lit, Meta};
use quote::ToTokens;
use super::{get_entity_meta_items, get_lit_str};
use super::{context::Context, symbol::{ENTITY, NAMESPACE, SET_NAME, Symbol}};

pub struct  Container {
    pub ident:          Ident,
    pub generics:       Generics,
    pub namespace:      String,
    pub set_name:       String,
}


impl Container {

    pub fn from_ast(ctx: &Context, item: &syn::DeriveInput) -> Self {
        let mut namespace = "test".to_owned();
        let mut set_name = "test".to_owned();

        for meta_item in item
            .attrs
            .iter()
            .flat_map(|attr| get_entity_meta_items(ctx, attr))
            .flatten()
        {
            match &meta_item {
                Meta(NameValue(m)) if m.path == NAMESPACE => {
                    if let Ok(s) = get_lit_str(ctx, NAMESPACE, NAMESPACE, &m.lit) {
                        namespace = s.value();
                    }
                },
                Meta(NameValue(m)) if m.path == SET_NAME => {
                    if let Ok(s) = get_lit_str(ctx, SET_NAME, SET_NAME, &m.lit) {
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
            set_name, 
        }
    }

}

