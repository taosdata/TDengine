from __future__ import annotations
import argparse

gConfig:    argparse.Namespace 

class Settings:
    @classmethod    
    def init(cls):
        global gConfig
        gConfig = []

    @classmethod
    def setConfig(cls, config):
        global gConfig
        gConfig = config