# Snowflute

### Getting Started

```
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### Overview

```
├── data/
│   └── geohashes.csv - used for finding geohashes of countries
├── helpers/
│   ├── general.py - contains general and geographical helper functions
│   └── snowflake.py - contains functions to interact with the snowflake api
├── .env.example - contains an example of what your .env file should look like
├── .gitignore
├── README.md
└── requirements.txt
```

### Setting Up Enviornment Variables

1. create a file in the root directory called .env
2. copy the contents of .env.example into .env
3. replace 'xxxxxx' with your credentials
