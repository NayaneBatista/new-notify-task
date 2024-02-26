import subprocess

# Lista de nomes dos arquivos Python a serem executados
arquivos_a_executar = ["recebe.py", "envia.py", "data.py", "user.py", "api.py"]

# Loop para executar cada arquivo em terminais separados
for arquivo in arquivos_a_executar:
    try:
        subprocess.Popen(["python", arquivo], shell=True)
    except Exception as e:
        print(f"Erro ao executar o arquivo {arquivo}: {str(e)}")
