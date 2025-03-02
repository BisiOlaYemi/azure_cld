from setuptools import setup, find_packages

setup(
    name="azure_data_pipeline",
    version="0.1.0",
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        "fastapi>=0.68.0",
        "uvicorn>=0.15.0",
        "python-multipart>=0.0.5",
        "pydantic>=1.8.2",
        "azure-storage-blob>=12.9.0",
        "azure-identity>=1.7.0",
        "azure-eventhub>=5.6.0",
        "azure-data-tables>=12.4.0",
        "azure-cosmos>=4.3.0",
        "pandas>=1.3.3",
        "pyarrow>=5.0.0",  
        "openpyxl>=3.0.9",  
        "sqlalchemy>=1.4.23",
        "asyncpg>=0.24.0", 
        "aiomysql>=0.0.22",  
        "aiohttp>=3.7.4",
        "python-dotenv>=0.19.0",
    ],
    python_requires=">=3.8",
    author="Yemi Ogunrinde",
    author_email="ogunrinde_olayemi@yahoo.com",
    description="Real-time data processing and migration to Azure cloud",
    keywords="azure, data, pipeline, fastapi",
    entry_points={
        "console_scripts": [
            "azure-data-pipeline=app.main:start",
        ],
    },
)