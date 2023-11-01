# machine-learning-suite

## Prerequisites

- Ensure you have `Homebrew` installed. If not, install it from [here](https://brew.sh/).

## Installation & Setup

### 1. Python Environment Setup

`pyenv` is a Python version management tool, allowing seamless switching between different Python versions.

- **Installation:**

  ```bash
  brew install pyenv
  ```

- **Install Python 3.10:**

  ```bash
  pyenv install 3.10
  ```

- **Set Python 3.10 as default:**

  ```bash
  pyenv global 3.10
  ```

- **Initialize `pyenv`:**

  ```bash
  eval "$(pyenv init --path)"
  ```

### 2. Virtual Environment Setup

Setting up a virtual environment ensures that dependencies related to different projects don't interfere with each other.

- **Creation:**

  ```bash
  python -m venv .venv
  ```

- **Activation:**

  ```bash
  source .venv/bin/activate
  ```

### 3. Install Dependencies

With the virtual environment activated, you can now install the project's dependencies:

```bash
pip install -r requirements.txt
```

## Tests & Quality

### Prerequisites

Initialise `pre-commit` utility.

```
pre-commit install
```

Check the local files without having to commit

```
pre-commit run --all-files
```

### Execute Tests

```bash
python -m pytest
```

### Execute Tests With Coverage Report

- **Execute the tests:**

  ```bash
  coverage run -a -m pytest test/
  ```

- **View the coverage report:**

  ```bash
  coverage report
  ```

- **Generate and open the HTML report:**

  ```bash
  coverage html
  open htmlcov/index.html
  ```
