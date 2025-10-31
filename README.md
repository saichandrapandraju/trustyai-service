# TrustyAI Service

👋 The TrustyAI Service is intended to be a hub for all kinds of Responsible AI workflows, such as
explainability, drift, and Large Language Model (LLM) evaluation. Designed as a REST server wrapping
a core Python library, the TrustyAI service is intended to be a tool that can operate in a local
environment, a Jupyter Notebook, or in Kubernetes.

---
## Native Algorithms
### 📈Drift  📉
- Fourier Maximum Mean Discrepancy (FourierMMD)
- Jensen-Shannon
- Approximate Kolmogorov–Smirnov Test
- Kolmogorov–Smirnov Test (KS-Test)
- Meanshift

### ⚖️ Fairness ⚖️
- Statistical Parity Difference
- Disparate Impact Ratio
- Average Odds Ratio (WIP)
- Average Predictive Value Difference (WIP)
- Individual Consistency (WIP)

---
## Imported Algorithms/Libraries
### 🔬Explainability 🔬
- [LIME](https://github.com/marcotcr/lime) (WIP)
- [SHAP](https://github.com/shap/shap) (WIP)

### 📋 LLM Evaluation  📋
- [LM-Evaluation-Harness](https://github.com/EleutherAI/lm-evaluation-harness/tree/main)

### 🛡️ Red Teaming 🛡️
- **Static Red Teaming**: Evaluate models against curated datasets of adversarial prompts
- **Dynamic Red Teaming (GOAT)**: Automated multi-turn adversarial attacks using attacker LLM with O-T-S-R reasoning
- **LLM-as-Judge**: Automated evaluation of attack success
- **Built-in datasets**: Jailbreak attempts, prompt injections, harmful content probes
- **Attack Techniques**: 7 techniques from GOAT paper (Refusal Suppression, Persona Modification, etc.)

---
## 📦 Building 📦
### Locally
```bash
uv pip install ".[$EXTRAS]"
```

### Container
```bash
podman build -t $IMAGE_NAME --build-arg EXTRAS="$EXTRAS" .
```

### Available Extras
Pass these extras as a comma separated list, e.g., `"mariadb,protobuf"`
* `protobuf`: To process model inference data from ModelMesh models, you can install with `protobuf` support. Otherwise, only KServe models will be supported.
* `eval`: To enable the Language Model Evaluation servers, install with `eval` support.
* `mariadb` (If installing locally, install the [MariaDB Connector/C](https://mariadb.com/docs/server/connect/programming-languages/c/install/) first.)
* `redteam`: To enable Red Teaming capabilities, install with `redteam` support. This includes OpenAI client (compatible with vLLM and other OpenAI-compatible endpoints).

### Examples
```bash
uv pip install ".[mariadb,protobuf,eval,redteam]"
podman build -t $IMAGE_NAME --build-arg EXTRAS="mariadb,protobuf,eval,redteam" .
```

## 🏃Running 🏃‍♀️
### Locally
```bash
uv run uvicorn src.main --host 0.0.0.0 --port 8080
```

### Container
```bash
podman run -t $IMAGE_NAME -p 8080:8080 .
```

### 🔐 TLS Support
The service supports TLS encryption and automatically detects certificates at startup:

- **With TLS certificates**: Runs on port 4443 (HTTPS)
- **Without TLS certificates**: Runs on port 8080 (HTTP)

**Certificate locations** (configurable via environment variables):
- Certificate: `/etc/tls/internal/tls.crt` (or `TLS_CERT_FILE`)
- Private key: `/etc/tls/internal/tls.key` (or `TLS_KEY_FILE`)

**Environment variables**:
- `TLS_CERT_FILE`: Path to TLS certificate file
- `TLS_KEY_FILE`: Path to TLS private key file
- `SSL_PORT`: HTTPS port (default: 4443)
- `HTTP_PORT`: HTTP port (default: 8080)

The TLS implementation is fully compatible with the TrustyAI operator for seamless Kubernetes deployment.

## 🧪 Testing 🧪
### Running All Tests
To run all tests in the project:
```bash
python -m pytest
```

Or with more verbose output:
```bash
python -m pytest -v
```

### Running with Coverage
To run tests with coverage reporting:
```bash
python -m pytest --cov=src
```

---
## 🔄 Protobuf Support 🔄
To process model inference data from ModelMesh models, you can install protobuf support. Otherwise, only KServe models will be supported.

### Generating Protobuf Code
After installing dependencies, generate Python code from the protobuf definitions:

```bash
# From the project root
bash scripts/generate_protos.sh
```

### Testing Protobuf Functionality
Run the tests for the protobuf implementation:

```bash
# From the project root
python -m pytest tests/service/data/test_modelmesh_parser.py -v
```

---
## 🛡️ Red Teaming 🛡️

Comprehensive automated red teaming for LLM safety testing. One simple API, maximum coverage.

### Quick Start

1. **Install with Red Teaming support:**
```bash
uv pip install ".[redteam]"
```

2. **Set up API keys:**
```bash
export OPENAI_API_KEY="your-openai-key"
```

3. **Run comprehensive evaluation:**

**Direct Testing Only (fast):**
```bash
curl -X POST "http://localhost:8080/redteam/evaluate" \
  -H "Content-Type: application/json" \
  -d '{
    "target_model": {
      "model_name": "gpt-3.5-turbo",
      "endpoint": "http://localhost:8081/v1"
    },
    "dataset_source_config": {
      "source": "jailbreakbench",
      "split": "harmful",
      "limit": 20
    },
    "judge_model": {
      "model_name": "gpt-4",
      "api_key": "'$OPENAI_API_KEY'"
    }
  }'
```

**Direct + Automated Testing (comprehensive):**
```bash
curl -X POST "http://localhost:8080/redteam/evaluate" \
  -H "Content-Type: application/json" \
  -d '{
    "target_model": {
      "model_name": "gpt-3.5-turbo",
      "endpoint": "http://localhost:8081/v1"
    },
    "attacker_model": {
      "model_name": "gpt-4",
      "api_key": "'$OPENAI_API_KEY'",
      "temperature": 1.0
    },
    "dataset_source_config": {
      "source": "jailbreakbench",
      "limit": 10
    },
    "judge_model": {
      "model_name": "gpt-4",
      "api_key": "'$OPENAI_API_KEY'"
    },
    "num_sessions": 2
  }'
```
→ Runs 10 direct tests + 20 automated GOAT sessions (10 goals × 2 trials)

**With Encoding Tests (test evasion techniques):**
```bash
curl -X POST "http://localhost:8080/redteam/evaluate" \
  -H "Content-Type: application/json" \
  -d '{
    "target_model": {...},
    "attack_vectors": ["prompt_injection"],
    "converters": ["base64", "rot13"],
    "judge_model": {...}
  }'
```
→ Tests each prompt 3 times: original + base64 + ROT13  
→ Shows which encoding bypasses safety

4. **Monitor progress:**
```bash
curl "http://localhost:8080/redteam/jobs/{job_id}"
# Shows: "progress": 45.0 (attacks completed across all types)
```

5. **Get results:**
```bash
curl "http://localhost:8080/redteam/jobs/{job_id}/results"
```

### Health Check

Before running an evaluation, verify your models are accessible:

```bash
# Check target model
curl -X POST "http://localhost:8080/redteam/health/target" \
  -H "Content-Type: application/json" \
  -d '{
    "model_type": "openai",
    "model_name": "gpt-3.5-turbo",
    "api_key": "your-key"
  }'

# Check judge model
curl -X POST "http://localhost:8080/redteam/health/judge" \
  -H "Content-Type: application/json" \
  -d '{
    "model_type": "openai",
    "model_name": "gpt-4",
    "api_key": "your-key"
  }'
```

### Features

- **Unified Experience**: Single API endpoint for comprehensive testing
- **Attack Vectors**: Structured testing for OWASP LLM vulnerabilities
  - Prompt Injection (5 techniques)
  - Jailbreak (6 techniques) 
  - Harmful Content (6 techniques)
- **Dual Attack Modes**:
  - **Direct Testing**: Test prompts directly (fast, deterministic)
  - **Automated Attacks**: GOAT algorithm with multi-turn conversations (thorough)
- **Encoding/Evasion Testing**: Opt-in converters to test bypasses
  - Base64 encoding
  - ROT13 cipher
  - Unicode homoglyphs
  - Leetspeak obfuscation
- **Research Datasets**: JailbreakBench (100 harmful behaviors), built-in datasets, custom uploads
- **Auto-Judge Selection**: Automatically selects appropriate judge
- **Overall Safety Score**: Single metric (0-1) representing model safety
- **Attack Comparison**: See which approach is most effective
- **Real-time Progress**: Unified tracking across all attack types
- **vLLM Support**: Works with any OpenAI-compatible endpoint
- **Streaming**: Handles slow models without gateway timeouts

### API Endpoints

**Main Endpoint:**
- `POST /redteam/evaluate` - Comprehensive red team evaluation (direct + optional automated)
- `GET /redteam/jobs/{job_id}` - Check job status and progress
- `GET /redteam/jobs/{job_id}/results` - Get complete results with safety score
- `DELETE /redteam/jobs/{job_id}` - Delete a job

**Dataset Management:**
- `GET /redteam/datasets` - List available datasets and sources
- `POST /redteam/datasets/load` - Preview dataset before testing
- `GET /redteam/datasets/sources` - Get info about dataset sources

### Example Results

**Direct Testing Only:**
```json
{
  "job_id": "abc-123",
  "status": "completed",
  "overall_safety_score": 0.85,
  "attacks": [
    {
      "attack_type": "jailbreakbench-direct",
      "success_rate": 0.15,
      "total_attempts": 20,
      "successful_attempts": 3
    }
  ],
  "worst_attack_type": "jailbreakbench-direct"
}
```

**Comprehensive (Direct + Automated):**
```json
{
  "job_id": "xyz-789",
  "status": "completed",
  "overall_safety_score": 0.75,
  "attacks": [
    {
      "attack_type": "jailbreakbench-direct",
      "success_rate": 0.15,
      "total_attempts": 10
    },
    {
      "attack_type": "goat",
      "success_rate": 0.25,
      "total_attempts": 20
    }
  ],
  "comparison": {
    "direct_success_rate": 0.15,
    "automated_success_rate": 0.25,
    "delta": 0.10
  },
  "worst_attack_type": "goat"
}
```
→ Safety score is 0.75 (based on worst attack: 1 - 0.25)

---
## ☎️ API ☎️
When the service is running, visit `localhost:8080/docs` to see the OpenAPI documentation!
