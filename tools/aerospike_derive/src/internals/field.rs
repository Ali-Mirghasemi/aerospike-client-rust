use quote::ToTokens;
use syn::Meta::{NameValue, Path};
use proc_macro2::{Ident, TokenStream};
use syn::{FieldsNamed, Generics, Type};
use syn::NestedMeta::{Lit, Meta};

use crate::internals::symbol::IGNORE;
use crate::internals::{get_entity_meta_items, symbol::RENAME, get_lit_str};

use super::container::Container;
use super::context::Context;
use super::symbol::KEY;


pub struct Field {
    ident:          Ident,
    field_type:     Type,
    name:           String,
    ignored:        bool,
    is_key:         bool,
}

pub struct Fields {
    pub fields:         Vec<Field>,
}

impl Field {
    pub fn new(ident: Ident, field_type: Type) -> Self {
        Self {
            ident: ident.clone(),
            field_type,
            name: ident.to_string(),
            ignored: false,
            is_key: false, 
        }
    }

    pub fn from_ast(ctx: &mut Context, fields: &FieldsNamed) -> Fields {
        let mut output = Fields::new();

        for field in fields.named.iter() {
            
            let mut temp_field = Field::new(field.ident.clone().unwrap(), field.ty.clone());

            for meta_item in field.attrs
                .iter()
                .flat_map(|attr| get_entity_meta_items(ctx, attr))
                .flatten() {
                
                match &meta_item {
                    Meta(NameValue(m)) if m.path == RENAME => {
                        if let Ok(s) = get_lit_str(ctx, RENAME, RENAME, &m.lit) {
                            temp_field.name = s.value();
                        }
                    },
                    Meta(Path(m)) if m == IGNORE => {
                        temp_field.ignored = true;
                    },
                    Meta(Path(m)) if m == KEY => {
                        temp_field.is_key = true;
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
            output.push(temp_field);
        }

        output
    }
}

impl Fields {
    pub fn new() -> Self {
        Self {
            fields: Vec::new(),
        }
    }

    pub fn push(&mut self, field: Field) {
        self.fields.push(field);
    }

    pub fn have_ignored(&self) -> bool {
        self.fields.iter().position(|x| x.ignored).is_some()
    }

    pub fn get_key_field(&self) -> Option<&Field> {
        if let Some(pos) = self.fields.iter().position(|x| x.is_key) {
            Some(&self.fields[pos])
        }
        else {
            None
        }
    }

    pub fn get_bins(&self) -> Vec<&Field> {
        self.fields.iter().filter(|x| !x.ignored).collect()
    }

    pub fn impl_key<'a>(&self, ctx: &'a mut Context, cont: &Container) -> Result<TokenStream, &'a Context> {
        let key_field;
        if let Some(key) = self.get_key_field() {
            key_field = key;
        } 
        else {
            ctx.error_spanned_by(
                "Fields",
                "key field not found"
            );
            return Err(ctx);
        }

        let model = cont.ident.clone();
        let (impl_generics, ty_generics, where_clause) = cont.generics.split_for_impl();
        let ident = key_field.ident.clone();
        let ty = key_field.field_type.clone();

        Ok(quote::quote! {
            impl #impl_generics ::aerospike::entity::IntoKey for #model #ty_generics #where_clause {
                type KeyType = #ty;
            
                fn get_key(val: Self::KeyType) -> ::aerospike::Key {
                    use ::aerospike::entity::Set;
                    ::aerospike::Key::new(Self::namespace(), Self::set_name(), ::aerospike::Value::from(val)).unwrap()
                }
                
                fn key(&self) -> ::aerospike::Key {
                    Self::get_key(self.#ident)
                }
            
                fn into_key(self) -> ::aerospike::Key {
                    Self::get_key(self.#ident)
                }
            }
        })
    }

    fn impl_bins_body(bins: &Vec<&Field>) -> TokenStream {
        let mut tokens = TokenStream::new();

        for item in bins {
            let ident = item.ident.clone();
            let name = item.name.clone();

            tokens.extend(quote::quote! {
                ::aerospike::Bin::new(#name, ::aerospike::Value::from(entity.#ident)),
            });
        }

        tokens
    }

    fn impl_bins_body_ref(bins: &Vec<&Field>) -> TokenStream {
        let mut tokens = TokenStream::new();

        for item in bins {
            let ident = item.ident.clone();
            let name = item.name.clone();

            tokens.extend(quote::quote! {
                ::aerospike::Bin::new(#name, ::aerospike::Value::from(&entity.#ident)),
            });
        }

        tokens
    }

    pub fn impl_bins<'a>(&self, ctx: &'a mut Context, cont: &Container) -> Result<TokenStream, &'a Context> {
        let bins = self.get_bins();
        if bins.len() == 0{
            ctx.error_spanned_by(
                "Bins",
                "There is no bins"
            );
            return Err(ctx);
        }

        let model = cont.ident.clone();
        let (impl_generics, ty_generics, where_clause) = cont.generics.split_for_impl();
        let body = Self::impl_bins_body(&bins);        
        let body_ref = Self::impl_bins_body_ref(&bins);

        Ok(quote::quote! {
            impl<'a> #impl_generics ::aerospike::entity::IntoBins<'a> for #model #ty_generics #where_clause {
                fn bins(entity: &Self) -> Vec<::aerospike::Bin<'a>> {
                    vec![
                        #body_ref
                    ]
                }
            
                fn into_bins(entity: Self) -> Vec<::aerospike::Bin<'a>> {
                    vec![
                        #body
                    ]
                }
            }
        })
    }

    fn impl_from_body(bins: &Vec<&Field>) -> TokenStream {
        let mut tokens = TokenStream::new();

        for item in bins {
            let ident = item.ident.clone();
            let name = item.name.clone();

            tokens.extend(quote::quote! {
                #ident: From::from(record.bins.get(#name).unwrap_or_default()),
            });
        }  

        tokens
    }

    pub fn impl_from_record<'a>(&self, ctx: &'a mut Context, cont: &Container) -> Result<TokenStream, &'a Context> {
        let bins = self.get_bins();
        if bins.len() == 0 {
            ctx.error_spanned_by(
                "Bins",
                "There is no bins"
            );
            return Err(ctx);
        }

        let model = cont.ident.clone();
        let (impl_generics, ty_generics, where_clause) = cont.generics.split_for_impl();
        let body = Self::impl_from_body(&bins);        

        Ok(quote::quote! {
            impl #impl_generics ::aerospike::entity::FromRecord for #model #ty_generics #where_clause {
                fn from_record(record: aerospike::Record) -> Self {
                    Self {
                        #body
                        ..Default::default()
                    }
                    
                }
            }
        })
    }

    pub fn impl_entity<'a>(&self, ctx: &'a mut Context, cont: &Container) -> Result<TokenStream, &'a Context> {
        let model = cont.ident.clone();
        let (impl_generics, ty_generics, where_clause) = cont.generics.split_for_impl();    

        Ok(quote::quote! {
            impl<'a> #impl_generics ::aerospike::entity::Entity<'a> for #model #ty_generics #where_clause {}
        })
    }

}
