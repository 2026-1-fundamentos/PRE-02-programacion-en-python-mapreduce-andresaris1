"""Taller evaluable"""

# pylint: disable=broad-exception-raised

import fileinput
import glob
import os.path
import time
from itertools import groupby

from toolz.itertoolz import concat, pluck



def copy_raw_files_to_input_folder(n):
    """Generate n copies of the raw files in the input folder"""
    
    # Se copian los archivos de `files/raw/` a `files/input/` para simular
    # un conjunto de entrada más grande y poder medir/ejecutar el job.
    raw_files = sorted(glob.glob(os.path.join("files", "raw", "*.txt")))
    if not raw_files:
        raise Exception("No se encontraron archivos en files/raw")

    input_dir = os.path.join("files", "input")
    os.makedirs(input_dir, exist_ok=True)

    for i in range(n):
        for raw_path in raw_files:
            base = os.path.basename(raw_path)
            name, ext = os.path.splitext(base)
            dst_path = os.path.join(input_dir, f"{name}-{i:04d}{ext}")
            with open(raw_path, "r", encoding="utf-8") as src, open(
                dst_path, "w", encoding="utf-8"
            ) as dst:
                dst.write(src.read())



def load_input(input_directory):
    """Funcion load_input"""

    pattern = os.path.join(input_directory, "*.txt")
    paths = sorted(glob.glob(pattern))
    if not paths:
        raise Exception(f"No se encontraron archivos de entrada en {input_directory}")

    # `fileinput` permite iterar líneas de múltiples archivos como si fuera uno solo.
    return fileinput.input(files=paths, openhook=fileinput.hook_encoded("utf-8"))


def preprocess_line(x):
    """Preprocess the line x"""

    # Este preprocesamiento hace que el conteo sea insensible a mayúsculas
    # y elimina signos de puntuación separándolos como espacios.
    x = x.lower()
    cleaned = []
    for ch in x:
        if "a" <= ch <= "z":
            cleaned.append(ch)
        else:
            cleaned.append(" ")
    return "".join(cleaned)


def map_line(x):
    """Convierte una línea en pares (palabra, 1)."""
    x = preprocess_line(x)
    for token in x.split():
        yield (token, 1)

def mapper(sequence):
    """Mapper: expande líneas a una secuencia de pares (palabra, 1)."""

    # `concat` aplana la secuencia de generadores que devuelve `map_line`.
    return concat(map(map_line, sequence))


def shuffle_and_sort(sequence):
    """Shuffle and Sort: ordena por clave para poder agrupar."""
    return sorted(sequence, key=lambda kv: kv[0])



def compute_sum_by_group(group):
    """Suma los valores de un grupo (misma palabra)."""
    key, values_iter = group
    total = 0
    for _, value in values_iter:
        total += value
    return (key, total)

def reducer(sequence):
    """Reducer: agrupa por palabra y calcula el total."""
    for group in groupby(sequence, key=lambda kv: kv[0]):
        yield compute_sum_by_group(group)


def create_directory(directory):
    """Create Output Directory"""
    os.makedirs(directory, exist_ok=True)


def save_output(output_directory, sequence):
    """Save Output"""
    output_path = os.path.join(output_directory, "part-00000")
    with open(output_path, "w", encoding="utf-8") as f:
        # `pluck` mantiene explícito qué columnas escribimos.
        for key, value in pluck([0, 1], sequence):
            f.write(f"{key}\t{value}\n")


def create_marker(output_directory):
    """Create Marker"""
    marker_path = os.path.join(output_directory, "_SUCCESS")
    with open(marker_path, "w", encoding="utf-8") as f:
        f.write("")


def run_job(input_directory, output_directory):
    """Job"""
    sequence = load_input(input_directory)
    sequence = mapper(sequence)
    sequence = shuffle_and_sort(sequence)
    sequence = reducer(sequence)
    create_directory(output_directory)
    save_output(output_directory, sequence)
    create_marker(output_directory)


if __name__ == "__main__":

    copy_raw_files_to_input_folder(n=1000)

    start_time = time.time()

    run_job(
        "files/input",
        "files/output",
    )

    end_time = time.time()
    print(f"Tiempo de ejecución: {end_time - start_time:.2f} segundos")
