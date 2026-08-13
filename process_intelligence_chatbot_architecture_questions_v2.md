# Process Intelligence Chatbot – Current Architecture and Critical Open Questions

## 1. Knowledge Ingestion Flow

```mermaid
flowchart TD
    A[User Uploads Process Content] --> A1[Process Description]
    A --> A2[SOP / Policy]
    A --> A3[BPMN / Process Diagram]
    A --> A4[Other Process Documents]

    A1 --> B[Document Ingestion Pipeline]
    A2 --> B
    A3 --> B
    A4 --> B

    B --> C[Parse and Normalize Content]
    C --> D[Document Metadata Enrichment]
    D --> E[Chunking Strategy?]

    E --> F[Create Chunks]
    F --> G[Generate Embeddings]
    G --> H[(Vector Store)]

    F --> I[Entity and Relationship Extraction]
    I --> J[Ontology / Entity Model?]
    J --> K[(Knowledge Graph)]

    I --> L[Map Chunk IDs to Entity / Relationship IDs]
    L --> H
    L --> K

    D --> M[Access / Process Permissions]
    M --> H
    M --> K

    H --> N[Validation / Testing?]
    K --> N
    N --> O{Ready for Search?}

    O -->|No| P[Draft / Restricted Knowledge]
    O -->|Yes| Q[Published Knowledge]
```

### Ingestion Steps

- User uploads process-related documents.
- Parse and normalize content from different document types.
- Add process, source, version, security, and other metadata.
- Break documents into searchable chunks.
- Generate embeddings and store chunks in the vector store.
- Extract entities and relationships from document content.
- Store entities and relationships in the knowledge graph.
- Connect chunks with graph entity and relationship IDs.
- Apply access-control metadata to both retrieval stores.
- Validate the ingested knowledge before making it searchable.

---

## 2. Real-Time Chat, Information Retrieval and Presentation Flow

```mermaid
flowchart TD
    U[User Question] --> A[Authentication and Authorization Context]
    A --> B[Intent Detection]

    B --> C{Question Type}

    C -->|Process Knowledge| D[Knowledge Retrieval Router]
    C -->|Running Process / Process Mining| E[Process Mining Tool Router]
    C -->|Both?| F[Combined Retrieval]

    D --> G{Vector First or Graph First?}

    G -->|Vector First| H[Vector / Hybrid Search]
    H --> I[Relevant Chunks]
    I --> J[Get Linked Entities / Relationships]
    J --> K[Graph Traversal?]

    G -->|Graph First| L[Graph Query / Traversal]
    L --> M[Relevant Entities / Relationships]
    M --> N[Retrieve Supporting Chunks?]

    K --> O[Knowledge Context]
    N --> O

    E --> P[Select Process Mining Tool]
    P --> Q[(Journey / Variant / Snapshot Index)]
    Q --> R[Journey, Variant, Frequency, Duration, Activities]

    F --> D
    F --> E

    O --> S[Context Aggregation]
    R --> S

    S --> T[LLM Response Generation]
    T --> V{Presentation Type?}

    V -->|Facts / Explanation| W[Text + Sources]
    V -->|Journey / Variant| X[Journey / Activity Map]
    V -->|Top Variants| Y[Table]
    V -->|Comparison / Trend| Z[Chart / Graph]
    V -->|Process Definition| AA[BPMN / Process Diagram]

    W --> AB[Final Response]
    X --> AB
    Y --> AB
    Z --> AB
    AA --> AB
```

### Runtime Steps

- Receive the user question with the user's authorization context.
- Detect whether the question is about documented process knowledge, observed process-mining data, or both.
- Route knowledge questions to vector search, graph search, or a combination.
- Use vector results to identify relevant graph nodes when appropriate.
- Use graph results to locate supporting document chunks when appropriate.
- Route process-mining questions to the required journey, variant, activity, or snapshot tools.
- Combine retrieved evidence before sending it to the LLM.
- Generate the response only from authorized retrieved information.
- Select the appropriate representation: text, table, chart, journey map, or BPMN/process diagram.

---

# 12 Critical Open Questions

1. **Chunking strategy?**
2. **Ontology / entity model?**
3. **Entity and relationship extraction accuracy?**
4. **Vector first or graph first?**
5. **When to stop graph traversal?**
6. **Graph-to-chunk and chunk-to-graph linking?**
7. **Hybrid retrieval and reranking strategy?**
8. **Access control across Vector DB + Graph DB + process-mining data?**
9. **Document validation before publishing to search?**
10. **Knowledge rollback and fix?**
10. **Process-mining tool boundaries and tool selection?**
11. **Representation selection: text, table, chart, journey map, BPMN?**
12. **Retrieval and answer accuracy evaluation?**
13. **How do we use users feedback to improve response?**
