# Kafka Connector

A **Go framework for building Kafka connectors (source and sink)**.
It supports **configuration fully compatible with the Kafka Connect REST API**, and **connectors are deployed as Go plugins** for flexible extension. The framework also includes encoding/decoding support, validation, and Docker-ready deployment.

Use it to:

* Stream data **into Kafka** (source connectors).
* Stream data **out of Kafka** (sink connectors).
* Apply transformations and validations.
* Deploy connectors as **Go plugins**.
* Manage tasks with sink/source runners.

---

## 🚀 Features

* **Source & Sink connectors** — read from and write to Kafka topics.
* **Config compatibility** — fully aligned with Kafka Connect REST API configs.
* **Go plugin deployment** — connectors are compiled and loaded as Go plugins.
* **Encoding/decoding support** — flexible data formats.
* **Pluggable design** — extend via registry and plugins.
* **Validation** — built-in config validation.
* **Container-ready** — deploy easily with Docker.

---

## 📦 Getting Started

### Prerequisites

* Go 1.23+
* Docker (optional, for containerized deployment)
* Running Kafka cluster

### Clone the repo

```bash
git clone https://github.com/noelyahan/kafka-connector.git
cd kafka-connector
```

### Build

```bash
go build ./...
```

### Run

```bash
go run main.go
```

---

## ⚙️ Configuration

Connector configuration is **fully compatible with the Kafka Connect REST API**.

Example (same format as you’d POST to Kafka Connect):

```json
{
  "name": "sample-sink",
  "config": {
    "connector.class": "FileStreamSink",
    "tasks.max": "1",
    "topics": "input-topic",
    "file": "/tmp/test.sink.txt"
  }
}
```

* **name** — connector name
* **config** — map of properties, identical to Kafka Connect REST API

This ensures full compatibility with existing Kafka Connect workflows and tools.

---

## 🔌 Go Plugin Deployment

Connectors are built as **Go plugins** (`.so` files) that can be dynamically loaded at runtime.

* Build a connector as a Go plugin:

  ```bash
  go build -buildmode=plugin -o my-connector.so ./my-connector
  ```
* Place the `.so` file in the designated plugin directory.
* Reference it in your connector configuration.

This makes it easy to extend the system with custom connectors without modifying the core runtime.

---

## 🐳 Docker

Build and run with Docker:

```bash
docker build -t kafka-connector .
docker run --rm kafka-connector
```

---

## 🧩 Extensibility

The framework supports **plugins and registry** for extending functionality:

* Add new encoders/decoders under `encoding/`.
* Add transformations under `transform/`.
* Register and load connectors dynamically as Go plugins.

---

## 📂 Project Structure

```
.
├── connector/       # Core source/sink connector logic
├── encoding/        # Encoding & decoding implementations
├── transform/       # Data transformations
├── Dockerfile       # Container build
├── go.mod           # Dependencies
└── main.go          # Entry point
```

---

## 🤝 Contributing

1. Fork the repo
2. Create a feature branch (`git checkout -b feature/my-feature`)
3. Commit changes (`git commit -m 'Add new feature'`)
4. Push to branch (`git push origin feature/my-feature`)
5. Open a Pull Request

---

## 📜 License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.

---

✅ Now the README emphasizes **Kafka Connect REST API configs** directly, instead of tying things to a `config.yaml` file.

Do you also want me to create a **sample plugin implementation** (e.g., a simple FileSink connector in Go) so new users see exactly how to write and load a connector?
