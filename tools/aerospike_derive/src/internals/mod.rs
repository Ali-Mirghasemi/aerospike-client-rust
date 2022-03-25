use self::{context::Context, symbol::{ENTITY, Symbol}};


pub mod attr;
pub mod container;
pub mod context;
pub mod symbol;
pub mod field;

pub fn get_entity_meta_items(cx: &Context, attr: &syn::Attribute) -> Result<Vec<syn::NestedMeta>, ()> {
    if attr.path != ENTITY {
        return Ok(Vec::new());
    }

    match attr.parse_meta() {
        Ok(syn::Meta::List(meta)) => Ok(meta.nested.into_iter().collect()),
        Ok(other) => {
            cx.error_spanned_by(other, "expected #[entity(...)]");
            Err(())
        }
        Err(err) => {
            cx.syn_error(err);
            Err(())
        }
    }
}

fn get_lit_str<'a>(
    ctx: &Context,
    attr_name: Symbol,
    meta_item_name: Symbol,
    lit: &'a syn::Lit,
) -> Result<&'a syn::LitStr, ()> {
    if let syn::Lit::Str(lit) = lit {
        Ok(lit)
    } else {
        ctx.error_spanned_by(
            lit,
            format!(
                "expected entity {} attribute to be a string: `{} = \"...\"`",
                attr_name, meta_item_name
            ),
        );
        Err(())
    }
}
