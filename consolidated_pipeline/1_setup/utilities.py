# Databricks notebook source
"""
Project Utilities and Shared Configuration

Purpose:
- Define shared schema names across all pipeline notebooks
- Provide centralized configuration for medallion architecture layers
- Enable consistent namespace usage across Bronze, Silver, and Gold layers
- Support modular and reusable pipeline development

Tech:
- Python configuration module
- Databricks %run import compatible
"""

bronze_schema = "bronze"
silver_schema = "silver"
gold_schema = "gold"