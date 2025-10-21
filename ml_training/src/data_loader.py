"""
Data Loader für DuckDB Training Data
Lädt Daten aus der DuckDB-Datenbank und bereitet sie für das Training vor.
"""
import duckdb
import pandas as pd
from typing import Dict, List, Tuple, Optional
from pathlib import Path
import yaml
from torch.utils.data import Dataset
import torch


class DebateDataset(Dataset):
    """PyTorch Dataset für Brexit Debate Daten"""
    
    def __init__(self, texts: List[str], labels: List[List[int]], tokenizer, max_length: int = 2048):
        self.texts = texts
        self.labels = labels
        self.tokenizer = tokenizer
        self.max_length = max_length
        
    def __len__(self):
        return len(self.texts)
    
    def __getitem__(self, idx):
        text = self.texts[idx]
        label = self.labels[idx]
        
        # Tokenisierung
        encoding = self.tokenizer(
            text,
            truncation=True,
            max_length=self.max_length,
            padding='max_length',
            return_tensors='pt'
        )
        
        return {
            'input_ids': encoding['input_ids'].squeeze(0),
            'attention_mask': encoding['attention_mask'].squeeze(0),
            'labels': torch.tensor(label, dtype=torch.float)
        }


class DuckDBDataLoader:
    """Lädt Daten aus DuckDB und erstellt PyTorch Datasets"""
    
    def __init__(self, config_path: str):
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.db_path = Path(config_path).parent.parent / self.config['data']['database_path']
        self.table_name = self.config['data']['table_name']
        self.text_column = self.config['data']['text_column']
        self.label_columns = self.config['data']['label_columns']
        self.split_column = self.config['data']['split_column']
        self.max_length = self.config['data']['max_length']
        
    def load_data(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Lädt Train und Test Daten aus DuckDB"""
        print(f"Lade Daten aus {self.db_path}...")
        
        conn = duckdb.connect(str(self.db_path), read_only=True)
        
        # Train Daten laden
        train_query = f"""
        SELECT * FROM {self.table_name}
        WHERE {self.split_column} = '{self.config['data']['train_split']}'
        """
        train_df = conn.execute(train_query).fetchdf()
        
        # Test Daten laden
        test_query = f"""
        SELECT * FROM {self.table_name}
        WHERE {self.split_column} = '{self.config['data']['test_split']}'
        """
        test_df = conn.execute(test_query).fetchdf()
        
        conn.close()
        
        print(f"Train Samples: {len(train_df)}")
        print(f"Test Samples: {len(test_df)}")
        
        return train_df, test_df
    
    def get_label_columns(self, df: pd.DataFrame) -> List[str]:
        """Identifiziert die Label-Spalten automatisch"""
        # Annahme: Label-Spalten enthalten 'label' im Namen oder sind numerisch
        label_cols = [col for col in df.columns 
                     if col != self.text_column and col != self.split_column]
        
        # Falls zu viele Spalten, nehme die ersten N Label-Spalten
        if len(label_cols) > self.label_columns:
            label_cols = label_cols[:self.label_columns]
        
        print(f"Label Spalten: {label_cols}")
        return label_cols
    
    def prepare_datasets(self, tokenizer) -> Tuple[DebateDataset, DebateDataset]:
        """Bereitet Train und Test Datasets vor"""
        train_df, test_df = self.load_data()
        
        # Label-Spalten identifizieren
        label_cols = self.get_label_columns(train_df)
        
        # Train Dataset
        train_texts = train_df[self.text_column].tolist()
        train_labels = train_df[label_cols].values.tolist()
        
        # Test Dataset
        test_texts = test_df[self.text_column].tolist()
        test_labels = test_df[label_cols].values.tolist()
        
        train_dataset = DebateDataset(
            train_texts, 
            train_labels, 
            tokenizer, 
            self.max_length
        )
        
        test_dataset = DebateDataset(
            test_texts, 
            test_labels, 
            tokenizer, 
            self.max_length
        )
        
        return train_dataset, test_dataset
    
    def get_label_info(self) -> Dict:
        """Gibt Informationen über die Labels zurück"""
        train_df, _ = self.load_data()
        label_cols = self.get_label_columns(train_df)
        
        info = {
            'num_labels': len(label_cols),
            'label_names': label_cols,
            'label_distribution': {}
        }
        
        for col in label_cols:
            info['label_distribution'][col] = train_df[col].value_counts().to_dict()
        
        return info


def main():
    """Test Funktion"""
    from transformers import AutoTokenizer
    
    config_path = "../config/training_config.yaml"
    
    loader = DuckDBDataLoader(config_path)
    
    # Label Info anzeigen
    label_info = loader.get_label_info()
    print("\nLabel Information:")
    print(f"Anzahl Labels: {label_info['num_labels']}")
    print(f"Label Namen: {label_info['label_names']}")
    
    # Beispiel: Dataset erstellen (benötigt Tokenizer)
    # tokenizer = AutoTokenizer.from_pretrained("openai-community/gpt-oss-20b")
    # train_dataset, test_dataset = loader.prepare_datasets(tokenizer)
    # print(f"\nDataset erstellt: {len(train_dataset)} train, {len(test_dataset)} test")


if __name__ == "__main__":
    main()
