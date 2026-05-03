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
    # Primary deployment — gpt-4o, used on turn 1
    AZURE_OPENAI_DEPLOYMENT: str = ""
    # Mini deployment — gpt-4o-mini, used on follow-up turns 2+
    AZURE_OPENAI_DEPLOYMENT_MINI: str = ""
    # Embedding model deployment
    AZURE_OPENAI_EMBEDDING_DEPLOYMENT: str = ""

    # .env aliases (AZURE_OPENAI_API_BASE / AZURE_OPENAI_API_KEY)
    AZURE_OPENAI_API_BASE: str = ""
    AZURE_OPENAI_API_KEY: str = ""
    AZURE_OPENAI_MODEL: str = ""  # legacy, ignored
    AZURE_OPENAI_API_VERSION: str = ""

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
