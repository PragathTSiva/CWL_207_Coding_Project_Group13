"""
Data cleaning utilities for processing and improving the quality of film datasets.
"""

import re
import pandas as pd
import numpy as np
from typing import Dict, List, Union, Any

def clean_dataset(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply a series of cleaning operations to the film dataset.
    
    Args:
        df: Input DataFrame with film data
        
    Returns:
        Cleaned DataFrame
    """
    # Make a copy to avoid modifying the original
    cleaned_df = df.copy()
    
    # Apply individual cleaning functions
    cleaned_df = clean_titles(cleaned_df)
    cleaned_df = standardize_imdb_ids(cleaned_df)
    cleaned_df = normalize_years(cleaned_df)
    cleaned_df = clean_summaries(cleaned_df)
    cleaned_df = normalize_people(cleaned_df)
    
    # Remove duplicate entries
    cleaned_df = cleaned_df.drop_duplicates(subset=['title'], keep='first')
    
    # Sort by year (descending) and title
    cleaned_df = cleaned_df.sort_values(by=['year', 'title'], ascending=[False, True], na_position='last')
    
    return cleaned_df

def clean_titles(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and standardize film titles."""
    # Make a copy to avoid modifying the original
    df = df.copy()
    
    # Fix common patterns in titles
    def clean_title(title):
        if pd.isna(title):
            return title
            
        # Remove trailing spaces
        title = title.strip()
        
        # Fix common patterns
        if " (film)" in title or " (movie)" in title:
            # Keep the part in parentheses to avoid ambiguity
            pass
        
        # Fix quotation marks
        title = title.replace('"', '"').replace('"', '"')
        
        return title
    
    df['title'] = df['title'].apply(clean_title)
    
    return df

def standardize_imdb_ids(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize IMDB IDs to ensure consistent format."""
    # Adding 'tt' prefix if missing - IMDB standard format
    df = df.copy()
    
    def standardize_imdb(imdb_id):
        if pd.isna(imdb_id):
            return imdb_id
            
        # Ensure string format
        imdb_id = str(imdb_id).strip()
        
        # Add tt prefix if missing
        if imdb_id and not imdb_id.startswith('tt'):
            imdb_id = 'tt' + imdb_id
            
        # Validate format
        if re.match(r'^tt\d+$', imdb_id):
            return imdb_id
        else:
            return None
    
    df['imdb_id'] = df['imdb_id'].apply(standardize_imdb)
    
    return df

def normalize_years(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize year values to ensure they are valid."""
    # Only accept years in reasonable range (1890-present)
    df = df.copy()
    
    def clean_year(year):
        if pd.isna(year):
            return year
            
        try:
            # Convert to float first (handles strings)
            year_float = float(year)
            
            # Convert to int (removes decimal part)
            year_int = int(year_float)
            
            # Validate year is in reasonable range (1890-present)
            current_year = pd.Timestamp.now().year
            if 1890 <= year_int <= current_year:
                return year_int
            else:
                return None
        except (ValueError, TypeError):
            return None
    
    df['year'] = df['year'].apply(clean_year)
    
    return df

def clean_summaries(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and standardize film summaries."""
    # Fix common issues in text summaries (whitespace, quotes, newlines)
    df = df.copy()
    
    def clean_summary(summary):
        if pd.isna(summary) or summary == "":
            return None
            
        # Fix common issues in summaries
        summary = summary.strip()
        
        # Fix quotation marks
        summary = summary.replace('"', '"').replace('"', '"')
        
        # Fix newlines
        summary = summary.replace('\n', ' ').replace('\r', '')
        
        # Fix multiple spaces
        summary = re.sub(r'\s+', ' ', summary)
        
        return summary
    
    df['summary'] = df['summary'].apply(clean_summary)
    
    return df

def normalize_people(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize the people field to ensure consistent format."""
    # Standardize semicolon-separated lists, remove duplicates
    df = df.copy()
    
    def clean_people(people):
        if pd.isna(people) or people == "":
            return None
            
        # Split by semicolon
        if isinstance(people, str):
            people_list = [p.strip() for p in people.split(';')]
            
            # Remove empty entries
            people_list = [p for p in people_list if p]
            
            # Remove duplicates and sort
            people_list = sorted(list(set(people_list)))
            
            # Join back with semicolons
            return '; '.join(people_list) if people_list else None
        
        return people
    
    df['people'] = df['people'].apply(clean_people)
    
    return df

def enrich_dataset(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add additional derived fields to the dataset.
    
    Args:
        df: Input DataFrame with film data
        
    Returns:
        Enriched DataFrame with additional columns
    """
    # Add computed columns to enhance analysis capabilities
    df = df.copy()
    
    # Add decade column based on year
    def extract_decade(year):
        if pd.isna(year):
            return None
        return int(year // 10 * 10)
    
    df['decade'] = df['year'].apply(extract_decade)
    
    # Count number of people associated with the film
    def count_people(people):
        if pd.isna(people):
            return 0
        return len(people.split(';'))
    
    df['people_count'] = df['people'].apply(count_people)
    
    # Determine if summary exists
    df['has_summary'] = ~df['summary'].isna()
    
    return df

def add_language_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extract language from film summaries where possible.
    
    Args:
        df: Input DataFrame with film data
        
    Returns:
        DataFrame with additional language column
    """
    # Use regex to extract language information from summaries
    df = df.copy()
    
    # Common Indian languages in films
    languages = [
        'Hindi', 'Tamil', 'Telugu', 'Malayalam', 'Kannada', 
        'Bengali', 'Marathi', 'Punjabi', 'Gujarati', 'Assamese',
        'Odia', 'Bhojpuri', 'Urdu'
    ]
    
    # Create a regex pattern to match these languages
    pattern = r'(?i)(' + '|'.join(languages) + r')[\s\-]language'
    
    def extract_language(row):
        if pd.isna(row['summary']):
            return None
            
        # Try to find language in summary
        match = re.search(pattern, row['summary'])
        if match:
            return match.group(1).title()
        
        return None
    
    df['language'] = df.apply(extract_language, axis=1)
    
    return df

def generate_data_quality_report(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Generate a quality report for the dataset.
    
    Args:
        df: Input DataFrame with film data
        
    Returns:
        Dictionary with quality metrics
    """
    # Generate stats for data quality assessment
    report = {
        'total_rows': len(df),
        'null_counts': {col: int(df[col].isna().sum()) for col in df.columns},
        'null_percentages': {col: round(df[col].isna().mean() * 100, 2) for col in df.columns},
        'duplicates': int(df.duplicated(subset=['title']).sum()),
        'year_range': (int(df['year'].min()) if not pd.isna(df['year'].min()) else None, 
                      int(df['year'].max()) if not pd.isna(df['year'].max()) else None),
        'decade_distribution': df['decade'].value_counts().to_dict() if 'decade' in df.columns else {},
    }
    
    return report 