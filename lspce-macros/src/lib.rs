extern crate proc_macro;
use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, parse_quote, ItemFn};

#[proc_macro_attribute]
pub fn defun_safe(_attr_ts: TokenStream, item_ts: TokenStream) -> TokenStream {
    let mut fn_item: ItemFn = parse_macro_input!(item_ts);

    // Wrap the original function body with safe_call
    let original_body = fn_item.block.clone();
    fn_item.block = parse_quote! {
        {
            crate::safe_call(|| #original_body)
        }
    };

    // Return the modified function as TokenStream
    quote! { #fn_item }.into()
}
