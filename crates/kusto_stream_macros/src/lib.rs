use proc_macro::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Fields, parse_macro_input};

#[proc_macro_derive(KustoRow)]
pub fn derive_kusto_response_row(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    let fields = match &input.data {
        Data::Struct(s) => match &s.fields {
            Fields::Named(named) => &named.named,
            _ => panic!("`KustoRow` only supports structs with named fields"),
        },
        _ => panic!("`KustoRow` can only be derived for structs"),
    };

    let column_validators = fields.iter().enumerate().map(|(i, f)| {
        let field_name = f.ident.as_ref().unwrap().to_string();
        let field_type = &f.ty;
        let field_type_str = quote!(#field_type).to_string();
        quote! {
            {
                let column = &columns[#i];
                if column.0 != #field_name {
                    return ::core::result::Result::Err(::kusto_stream::Error::RowType {
                        reason: ::std::format!("Struct Field to Column {} Name Mismatch - Expected: {}, Got: {}", #i, #field_name, column.0)
                    });
                }
                if !<#field_type as ::kusto_stream::KustoScalar>::accepts(&column.1) {
                    return ::core::result::Result::Err(::kusto_stream::Error::RowType {
                        reason: ::std::format!("Struct Field to Column {} Type Mismatch - Expected: {}, Got: {}", #i, #field_type_str, column.1)
                    });
                }
            }
        }
    });

    let fields_count = fields.len();
    let (impl_generics, type_generics, where_clause) = input.generics.split_for_impl();

    quote! {
        impl #impl_generics ::kusto_stream::KustoRow for #name #type_generics #where_clause {
            fn validate(columns: &[(&str, &str)]) -> ::core::result::Result<(), ::kusto_stream::Error> {
                if columns.len() != #fields_count {
                    return ::core::result::Result::Err(::kusto_stream::Error::RowType {
                        reason: ::std::format!("Struct Fields to Response Columns Count Mismatch - Expected: {}, Got: {}", #fields_count, columns.len())
                    })
                }
                #( #column_validators )*
                ::core::result::Result::Ok(())
            }
        }
    }
    .into()
}
