import sys
import os

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(BASE_DIR)

from modelo import ProductoEnum
print(ProductoEnum.DIESEL_A1.value)