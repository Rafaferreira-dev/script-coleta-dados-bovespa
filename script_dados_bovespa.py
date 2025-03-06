"""
Script para coleta de dados do índice Bovespa (^BVSP) usando a biblioteca yfinance.
Salva os dados em um arquivo CSV e gera um log de execução.

Autor: Rafael Ferreira 
Data: 2024-05-20
"""

import yfinance as yf
import pandas as pd
import logging
from datetime import datetime
from pathlib import Path

# Configurações iniciais
TICKER = "^BVSP"
DATA_DIR = Path("data")
LOG_DIR = Path("logs")

def setup_directories():
    """Cria os diretórios para dados e logs, se não existirem."""
    DATA_DIR.mkdir(exist_ok=True)
    LOG_DIR.mkdir(exist_ok=True)

def configure_logging():
    """Configura o sistema de logging."""
    logging.basicConfig(
        filename=LOG_DIR / "bovespa_data.log",
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )

def fetch_bovespa_data(period: str = "1mo") -> pd.DataFrame:
    """
    Coleta dados históricos do índice Bovespa.
    
    Args:
        period: Período dos dados (ex: '1d', '1mo', '1y').
    
    Returns:
        DataFrame do Pandas com os dados históricos.
    """
    try:
        logging.info(f"Iniciando coleta de dados para {TICKER} (período: {period})")
        ticker = yf.Ticker(TICKER)
        data = ticker.history(period=period)
        
        if data.empty:
            logging.warning("Nenhum dado foi retornado pela API.")
            return pd.DataFrame()
            
        logging.info("Dados coletados com sucesso.")
        return data
    
    except Exception as e:
        logging.error(f"Erro na coleta de dados: {str(e)}")
        return pd.DataFrame()

def save_data(data: pd.DataFrame):
    """Salva os dados em um arquivo CSV com timestamp."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = DATA_DIR / f"bovespa_{timestamp}.csv"
    data.to_csv(filename, index=True)
    logging.info(f"Dados salvos em {filename}")

if __name__ == "__main__":
    setup_directories()
    configure_logging()
    
    bovespa_data = fetch_bovespa_data(period="1mo")
    
    if not bovespa_data.empty:
        print("Primeiras linhas dos dados:")
        print(bovespa_data.head())
        save_data(bovespa_data)
    else:
        print("Falha na coleta de dados. Verifique os logs.")