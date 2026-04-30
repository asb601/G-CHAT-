from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    # Database
    DATABASE_URL: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/genchatbot"

    # Google OAuth
    GOOGLE_CLIENT_ID: str = ""
    GOOGLE_CLIENT_SECRET: str = ""

    # JWT
    SECRET_KEY: str = "change-me-in-production"
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 60 * 24 * 7  # 7 days

    # Admin
    ADMIN_EMAIL: str = ""

    # Azure OpenAI
    AZURE_OPENAI_ENDPOINT: str = ""
    AZURE_OPENAI_KEY: str = ""
    AZURE_OPENAI_DEPLOYMENT: str = "gpt-4"
    # Embedding model deployment name — must be deployed in the same Azure OpenAI resource.
    # Standard name: text-embedding-3-small  (deploy it once in Azure Portal, paste name here)
    AZURE_OPENAI_EMBEDDING_DEPLOYMENT: str = "text-embedding-3-small"

    # .env aliases (AZURE_OPENAI_API_BASE / AZURE_OPENAI_API_KEY / AZURE_OPENAI_MODEL)
    AZURE_OPENAI_API_BASE: str = ""
    AZURE_OPENAI_API_KEY: str = ""
    AZURE_OPENAI_MODEL: str = ""
    AZURE_OPENAI_API_VERSION: str = "2024-02-01"

    # SMTP (Gmail App Password recommended)
    SMTP_HOST: str = "smtp.gmail.com"
    SMTP_PORT: int = 587
    SMTP_USER: str = ""
    SMTP_PASSWORD: str = ""

    # CORS
    FRONTEND_URL: str = "http://localhost:3000"

    # Storage encryption key (Fernet) — protects Azure connection strings at rest
    # Generate with: python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
    STORAGE_ENCRYPTION_KEY: str = ""

    model_config = {"env_file": ".env", "extra": "ignore"}


@lru_cache
def get_settings() -> Settings:
    return Settings()
