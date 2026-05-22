use proc_macro::TokenStream;
use quote::{format_ident, quote};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
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
    stages: StageDecls,
    graph: Option<Vec<Edge>>,
}

enum StageDecls {
    Linear(Vec<Expr>),
    Named(Vec<NamedStage>),
}

struct NamedStage {
    name: Ident,
    expr: Expr,
}

#[derive(Clone)]
enum Endpoint {
    Input,
    Output,
    Stage(Ident),
}

struct EndpointList {
    endpoints: Vec<Endpoint>,
}

struct Edge {
    from: EndpointList,
    to: EndpointList,
}

impl Parse for Endpoint {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let ident: Ident = input.parse()?;
        match ident.to_string().as_str() {
            "input" => Ok(Endpoint::Input),
            "output" => Ok(Endpoint::Output),
            _ => Ok(Endpoint::Stage(ident)),
        }
    }
}

impl Parse for EndpointList {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        if input.peek(syn::token::Bracket) {
            let content;
            bracketed!(content in input);
            let endpoints = Punctuated::<Endpoint, Token![,]>::parse_terminated(&content)?;
            Ok(EndpointList {
                endpoints: endpoints.into_iter().collect(),
            })
        } else {
            Ok(EndpointList {
                endpoints: vec![input.parse()?],
            })
        }
    }
}

impl Parse for Edge {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let from = input.parse()?;
        input.parse::<Token![->]>()?;
        let to = input.parse()?;
        Ok(Edge { from, to })
    }
}

impl Parse for NamedStage {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let name = input.parse()?;
        input.parse::<Token![=]>()?;
        let expr = input.parse()?;
        Ok(NamedStage { name, expr })
    }
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
        let mut graph = None;

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
                    if content.peek(syn::token::Bracket) {
                        let stage_content;
                        bracketed!(stage_content in content);
                        let parsed =
                            Punctuated::<Expr, Token![,]>::parse_terminated(&stage_content)?;
                        stages = Some(StageDecls::Linear(parsed.into_iter().collect()));
                    } else {
                        let stage_content;
                        braced!(stage_content in content);
                        let parsed =
                            Punctuated::<NamedStage, Token![,]>::parse_terminated(&stage_content)?;
                        stages = Some(StageDecls::Named(parsed.into_iter().collect()));
                    }
                    content.parse::<Token![;]>()?;
                }
                "graph" => {
                    let graph_content;
                    braced!(graph_content in content);
                    let mut edges = Vec::new();
                    while !graph_content.is_empty() {
                        edges.push(graph_content.parse()?);
                        if graph_content.is_empty() {
                            break;
                        }
                        graph_content.parse::<Token![;]>()?;
                    }
                    graph = Some(edges);
                    content.parse::<Token![;]>()?;
                }
                _ => {
                    return Err(syn::Error::new(
                        key.span(),
                        "expected config, stages, or graph assignment",
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
                .ok_or_else(|| syn::Error::new(content.span(), "missing `stages = ...;`"))?,
            graph,
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
        graph,
    } = parse_macro_input!(input as PipelineInput);

    let build_graph = match (stages, graph) {
        (StageDecls::Linear(stages), None) => expand_linear_graph(&input, &output, &error, stages),
        (StageDecls::Named(stages), Some(edges)) => {
            match expand_named_graph(&input, &output, &error, stages, edges) {
                Ok(tokens) => tokens,
                Err(error) => return error.to_compile_error().into(),
            }
        }
        (StageDecls::Linear(_), Some(_)) => {
            return syn::Error::new_spanned(
                name,
                "`graph = ...` requires named `stages = { ... };`",
            )
            .to_compile_error()
            .into();
        }
        (StageDecls::Named(_), None) => {
            return syn::Error::new_spanned(name, "named stages require `graph = { ... };`")
                .to_compile_error()
                .into();
        }
    };

    quote! {
        #vis struct #name;

        impl #name {
            pub fn start() -> ::piper::Result<::piper::Piper<#input, #output, #error>, #error> {
                #build_graph
                ::piper::Piper::start(#config, __piper_graph)
            }
        }
    }
    .into()
}

fn expand_linear_graph(
    input: &Type,
    output: &Type,
    error: &Type,
    stages: Vec<Expr>,
) -> proc_macro2::TokenStream {
    let mut tokens = quote! {
        let mut __piper_builder = ::piper::PipelineGraphBuilder::<#input, #error>::new();
        let __piper_link_0 = __piper_builder.input();
    };
    let mut previous = format_ident!("__piper_link_0");
    for (index, stage) in stages.into_iter().enumerate() {
        let next = format_ident!("__piper_link_{}", index + 1);
        tokens.extend(quote! {
            let #next = __piper_builder.add_stage(#previous, #stage);
        });
        previous = next;
    }
    tokens.extend(quote! {
        let __piper_graph = __piper_builder.finish::<#output>(#previous);
    });
    tokens
}

fn expand_named_graph(
    input: &Type,
    output: &Type,
    error: &Type,
    stages: Vec<NamedStage>,
    edges: Vec<Edge>,
) -> Result<proc_macro2::TokenStream> {
    let declared: HashSet<String> = stages.iter().map(|stage| stage.name.to_string()).collect();
    let mut parent = HashMap::<String, String>::new();
    let mut used_inputs = HashSet::<String>::new();
    let mut used_outputs = HashSet::<String>::new();
    let mut adjacency = HashMap::<String, Vec<String>>::new();

    insert_key(&mut parent, "input:out");
    insert_key(&mut parent, "output:in");
    for stage in &stages {
        insert_key(&mut parent, &format!("{}:in", stage.name));
        insert_key(&mut parent, &format!("{}:out", stage.name));
    }

    for edge in &edges {
        for from in &edge.from.endpoints {
            validate_endpoint(from, &declared)?;
            let from_key = source_key(from)?;
            used_outputs.insert(from_key.clone());
            for to in &edge.to.endpoints {
                validate_endpoint(to, &declared)?;
                let to_key = dest_key(to)?;
                used_inputs.insert(to_key.clone());
                union(&mut parent, &from_key, &to_key);
                if let (Endpoint::Stage(from_stage), Endpoint::Stage(to_stage)) = (from, to) {
                    adjacency
                        .entry(from_stage.to_string())
                        .or_default()
                        .push(to_stage.to_string());
                }
            }
        }
    }

    if !used_outputs.contains("input:out") {
        return Err(syn::Error::new_spanned(
            &stages[0].name,
            "graph must connect `input` to at least one stage",
        ));
    }
    if !used_inputs.contains("output:in") {
        return Err(syn::Error::new_spanned(
            &stages[0].name,
            "graph must connect at least one stage to `output`",
        ));
    }
    for stage in &stages {
        let input_key = format!("{}:in", stage.name);
        let output_key = format!("{}:out", stage.name);
        if !used_inputs.contains(&input_key) {
            return Err(syn::Error::new_spanned(
                &stage.name,
                "stage is missing an input graph edge",
            ));
        }
        if !used_outputs.contains(&output_key) {
            return Err(syn::Error::new_spanned(
                &stage.name,
                "stage is missing an output graph edge",
            ));
        }
    }
    detect_cycles(&stages, &adjacency)?;

    let mut root_to_ident = BTreeMap::<String, Ident>::new();
    let mut roots = BTreeSet::new();
    let keys: Vec<_> = parent.keys().cloned().collect();
    for key in keys {
        roots.insert(find(&mut parent, &key));
    }
    for (index, root) in roots.into_iter().enumerate() {
        root_to_ident.insert(root, format_ident!("__piper_link_{index}"));
    }

    let input_root = find(&mut parent, "input:out");
    let output_root = find(&mut parent, "output:in");
    let input_link = root_to_ident.get(&input_root).expect("input root exists");
    let output_link = root_to_ident.get(&output_root).expect("output root exists");

    let mut link_decls = quote! {};
    for (root, ident) in &root_to_ident {
        if root == &input_root {
            link_decls.extend(quote! {
                let #ident = __piper_builder.input();
            });
        } else {
            link_decls.extend(quote! {
                let #ident = __piper_builder.link();
            });
        }
    }

    let mut stage_decls = quote! {};
    for stage in stages {
        let name = stage.name;
        let expr = stage.expr;
        let in_root = find(&mut parent, &format!("{name}:in"));
        let out_root = find(&mut parent, &format!("{name}:out"));
        let in_link = root_to_ident
            .get(&in_root)
            .expect("stage input root exists");
        let out_link = root_to_ident
            .get(&out_root)
            .expect("stage output root exists");
        stage_decls.extend(quote! {
            let #name = #expr;
            __piper_builder.add_stage_to(#in_link, #name, #out_link);
        });
    }

    Ok(quote! {
        let mut __piper_builder = ::piper::PipelineGraphBuilder::<#input, #error>::new();
        #link_decls
        #stage_decls
        let __piper_graph = __piper_builder.finish::<#output>(#output_link);
        let _ = #input_link;
    })
}

fn validate_endpoint(endpoint: &Endpoint, declared: &HashSet<String>) -> Result<()> {
    if let Endpoint::Stage(stage) = endpoint {
        if !declared.contains(&stage.to_string()) {
            return Err(syn::Error::new_spanned(stage, "unknown graph stage"));
        }
    }
    Ok(())
}

fn source_key(endpoint: &Endpoint) -> Result<String> {
    match endpoint {
        Endpoint::Input => Ok("input:out".to_string()),
        Endpoint::Output => Err(syn::Error::new_spanned(
            quote!(output),
            "`output` cannot be used as a graph edge source",
        )),
        Endpoint::Stage(stage) => Ok(format!("{stage}:out")),
    }
}

fn dest_key(endpoint: &Endpoint) -> Result<String> {
    match endpoint {
        Endpoint::Input => Err(syn::Error::new_spanned(
            quote!(input),
            "`input` cannot be used as a graph edge destination",
        )),
        Endpoint::Output => Ok("output:in".to_string()),
        Endpoint::Stage(stage) => Ok(format!("{stage}:in")),
    }
}

fn insert_key(parent: &mut HashMap<String, String>, key: &str) {
    parent.insert(key.to_string(), key.to_string());
}

fn find(parent: &mut HashMap<String, String>, key: &str) -> String {
    let current = parent.get(key).cloned().unwrap_or_else(|| key.to_string());
    if current == key {
        current
    } else {
        let root = find(parent, &current);
        parent.insert(key.to_string(), root.clone());
        root
    }
}

fn union(parent: &mut HashMap<String, String>, left: &str, right: &str) {
    let left_root = find(parent, left);
    let right_root = find(parent, right);
    if left_root != right_root {
        parent.insert(right_root, left_root);
    }
}

fn detect_cycles(stages: &[NamedStage], adjacency: &HashMap<String, Vec<String>>) -> Result<()> {
    fn visit(
        node: &str,
        adjacency: &HashMap<String, Vec<String>>,
        temporary: &mut HashSet<String>,
        permanent: &mut HashSet<String>,
    ) -> bool {
        if permanent.contains(node) {
            return false;
        }
        if !temporary.insert(node.to_string()) {
            return true;
        }
        if let Some(next) = adjacency.get(node) {
            for child in next {
                if visit(child, adjacency, temporary, permanent) {
                    return true;
                }
            }
        }
        temporary.remove(node);
        permanent.insert(node.to_string());
        false
    }

    let mut temporary = HashSet::new();
    let mut permanent = HashSet::new();
    for stage in stages {
        if visit(
            &stage.name.to_string(),
            adjacency,
            &mut temporary,
            &mut permanent,
        ) {
            return Err(syn::Error::new_spanned(
                &stage.name,
                "graph cycles are not supported",
            ));
        }
    }
    Ok(())
}
