use proc_macro2::{TokenStream};
use syn::{DeriveInput, Data, Fields};
use quote::quote;

use crate::internals::{context::Context, container::Container, field::Field};



pub fn expand_derive_entity(input: &mut DeriveInput) -> Result<TokenStream, Vec<syn::Error>> {
    let mut ctx = Context::new();
    let cont = Container::from_ast(&ctx, input);
    let mut tokens = TokenStream::default();
    
    match &input.data {
        Data::Struct(data) => {
            match &data.fields {
                Fields::Named(fields) => {
                    let fields = Field::from_ast(&mut ctx, fields);
                    // implement set trait
                    tokens.extend(expand_derive_set(&cont));
                    // impl key trait
                    tokens.extend(fields.impl_key(&mut ctx, &cont)?);
                    // impl bins
                    tokens.extend(fields.impl_bins(&mut ctx, &cont)?);
                    // impl from_record
                    tokens.extend(fields.impl_from_record(&mut ctx, &cont)?);
                    // impl entity
                    tokens.extend(fields.impl_entity(&mut ctx, &cont)?);
                },
                Fields::Unnamed(_) => ctx.error_spanned_by(
                    &input,
                    "Unnamed fields not supported",
                ),
                Fields::Unit => ctx.error_spanned_by(
                    &input,
                    "Unit Fields not supported",
                ),
            }
        },
        Data::Enum(_) | Data::Union(_) => {
            ctx.error_spanned_by(
                &input,
                "Enum and Union not supported",
            );
        }
    }
    ctx.check()?;

    Ok(
        tokens
    )
}


pub fn expand_derive_set(cont: &Container) -> TokenStream {
    let ident = cont.ident.clone();
    let (impl_generics, ty_generics, where_clause) = cont.generics.split_for_impl();

    let namespace: TokenStream = if let Some(path) = &cont.namespace_fn {
        quote! { #path() }
    } 
    else {
        let ns = cont.namespace.clone();
        quote! { #ns }
    };

    let set_name: TokenStream = if let Some(path) = &cont.set_name_fn {
        quote! { #path() }
    } 
    else {
        let sn = cont.set_name.clone();
        quote! { #sn }
    };

    quote! {
        
        impl #impl_generics ::aerospike::entity::Set for #ident #ty_generics #where_clause {
            type Output = &'static str;
            
            fn namespace() -> Self::Output {
                #namespace
            }
        
            fn set_name() -> Self::Output {
                #set_name
            }
        }
    }.into()
}
