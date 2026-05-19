use proc_macro::TokenStream;
use quote::quote;
use syn::parse::{Parse, ParseStream};
use syn::punctuated::Punctuated;
use syn::{Expr, Ident, Result, Token, Type, Visibility, braced, bracketed, parse_macro_input};

struct PipelineInput {
    vis: Visibility,
    name: Ident,
    input: Type,
    output: Type,
    error: Type,
    config: Expr,
    stages: Vec<Expr>,
}

impl Parse for PipelineInput {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let vis = input.parse()?;
        input.parse::<Token![struct]>()?;
        let name = input.parse()?;

        let content;
        braced!(content in input);

        let mut pipeline_input = None;
        let mut output = None;
        let mut error = None;
        let mut config = None;
        let mut stages = None;

        while !content.is_empty() {
            if content.peek(Token![type]) {
                content.parse::<Token![type]>()?;
                let key: Ident = content.parse()?;
                content.parse::<Token![=]>()?;
                let value: Type = content.parse()?;
                content.parse::<Token![;]>()?;

                match key.to_string().as_str() {
                    "Input" => pipeline_input = Some(value),
                    "Output" => output = Some(value),
                    "Error" => error = Some(value),
                    _ => {
                        return Err(syn::Error::new(
                            key.span(),
                            "expected Input, Output, or Error",
                        ));
                    }
                }
                continue;
            }

            let key: Ident = content.parse()?;
            content.parse::<Token![=]>()?;

            match key.to_string().as_str() {
                "config" => {
                    config = Some(content.parse()?);
                    content.parse::<Token![;]>()?;
                }
                "stages" => {
                    let stage_content;
                    bracketed!(stage_content in content);
                    let parsed = Punctuated::<Expr, Token![,]>::parse_terminated(&stage_content)?;
                    stages = Some(parsed.into_iter().collect());
                    content.parse::<Token![;]>()?;
                }
                _ => {
                    return Err(syn::Error::new(
                        key.span(),
                        "expected config or stages assignment",
                    ));
                }
            }
        }

        Ok(PipelineInput {
            vis,
            name,
            input: pipeline_input
                .ok_or_else(|| syn::Error::new(content.span(), "missing `type Input = ...;`"))?,
            output: output
                .ok_or_else(|| syn::Error::new(content.span(), "missing `type Output = ...;`"))?,
            error: error
                .ok_or_else(|| syn::Error::new(content.span(), "missing `type Error = ...;`"))?,
            config: config
                .ok_or_else(|| syn::Error::new(content.span(), "missing `config = ...;`"))?,
            stages: stages
                .ok_or_else(|| syn::Error::new(content.span(), "missing `stages = [...]`;"))?,
        })
    }
}

#[proc_macro]
pub fn pipeline(input: TokenStream) -> TokenStream {
    let PipelineInput {
        vis,
        name,
        input,
        output,
        error,
        config,
        stages,
    } = parse_macro_input!(input as PipelineInput);

    quote! {
        #vis struct #name;

        impl #name {
            pub fn start() -> ::piper::Result<::piper::Piper<#input, #output, #error>, #error> {
                ::piper::Piper::start(#config, vec![#(#stages),*])
            }
        }
    }
    .into()
}
