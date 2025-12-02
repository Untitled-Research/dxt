# XLT — The YAML-first Data Move Tool
*Pronounced: ex-ell-tee*

> **XLT is a lightweight Extract–Load tool for building simple, reliable batch data movement pipelines.**  
> Write a single YAML file that defines an extract and a load step, and run it anywhere with a clean, fast CLI.

XLT gives data engineers a predictable, extensible, Git-native way to move data between databases, APIs, files, and cloud warehouses — without operating a control plane or writing yet another custom Python script.

---

## 🚀 Features

- **YAML-first pipelines** — define your extract and load declaratively  
- **CLI-first design** — run pipelines from the command line or any orchestrator  
- **Extensible** — create custom extractors/loaders with minimal Python  
- **Portable** — works anywhere Python runs (local, cron, Airflow, Prefect, GitHub Actions, on-prem)  
- **Incremental loads** — simple watermark & key-based incremental patterns  
- **Load strategies** — append, truncate-insert, merge (planned)  
- **Zero platform overhead** — no UI, no scheduler, no metadata DB  
- **Engineer-friendly** — CLI-first, Git-native, orchestration-agnostic  

If you’ve ever written a one-off Python script to copy data from A to B, XLT replaces that with a clean YAML pipeline and a battle-tested execution engine.

---

## Design Philosophy

XLT aims to occupy the gap between:

“I’ll write another custom Python script…”
and
“Let’s deploy Airbyte / a managed ELT platform.”

Principles:

- Simplicity over magic
- Declarative over imperative
- Small surface area
- No servers
- Do one thing well: move data

If dbt is the “data build tool”, XLT is the data move tool.

## 📦 Installation

```bash
pip install xlt
```

## 🤝 Contributing

XLT is designed to be open, simple, and community-friendly.

Ways to contribute:

- Create a new adapter (database, file, API, cloud service)
- Improve docs and examples
- Add validator rules for YAML schema
- Build testing fixtures
- Submit ideas for features

PRs are welcome!

## 📄 License

MIT License.