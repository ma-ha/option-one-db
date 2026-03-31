# AI Search

Option One DB has a built in query with AI support.

This enables queries as questions or search for similar text. 

To use AI search, the DB servers must have an LLM API URL configured (see setup)

## Creating an AI index

You can create AI index for any field, but text fields make most sense.

Example:

    await myColl.createIndex( 'productDescription', { AI: 'embedding-gemma' } ) 

Required disk space: 
Embeddings have a typical size of 13 kB. For each indexed document one or more embeddings are stored on disk.

Limitation: Currently only top level fields are supported. 
TODO: Implement indexing of e.g. "docu/technical-spec" or so.

## Using AI search

To search using LLM indexes use `$ai`. Example:

    query = {
      productDescription: {
        $ai: "Which products with suitable for new customers?"
      }
    }

You can specify a threshold `q` for the result quality (range is 0 to 1). 
Example:

    query = {
      productDescription: {
        $ai: "Which products with suitable for new customers?",
        q: 0.85
      }
    }

AI queries can be combined with classic queries (see [[query.md|query documents]]).

    query = {
      category: 'IT',
      price: { $lt: 200 },
      productDescription: {
        $ai: "Which products with suitable for new customers?"
      }
    }

Hint: 
The order of the fields in the query might have a huge impact on performance.
Its recommended to have the simplest queries with the smallest result set first.



## How is AI search working?

Basically a LLM server generates "embeddings" for document fields which are stored as index for a collection. This is a vector or point in space with lot dimensions. If the content of the field is very long, multiple embeddings are  generated for sub-parts.

The queries is also transformed in an embedding.
Embeddings of documents with a short distance to the query embedding are returned as search result. The maximum distance can be defined in the query.

## Enable AI search for Option One DB servers

AI indexing and search can be simply enabled by setting the `EMBEDDING_GEMMA_API` environment variable for the DB server with the URL of the LLM API server.

Example:

    export EMBEDDING_GEMMA_API="http://localhost:11434/api/embed"


## Set up a LLM API server

### Local PC (Linux)

Before getting started, ensure you have:

* Node.js installed
* Sufficient RAM (16 GB minimum recommended)
* Sufficient free disk space (10 GB recommended)

Step 1: Install Ollama

    curl -fsSL https://ollama.com/install.sh | sh


Step 2: If you have no GPU 

    export OLLAMA_CPU_WARNING=1

Step 3: Download the model

    ollama pull embeddinggemma


Step 4: Start Ollama

    ollama run embeddinggemma "test"

Ensure the API is working:

      curl http://localhost:11434/api/embed -d '{
        "model": "embeddinggemma",
        "input": "Why is the sky blue?"
      }'


### Kubernetes: LLM server cluster

TODO