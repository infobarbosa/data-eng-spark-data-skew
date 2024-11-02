import random
import csv

def gerar_dados(caminho_arquivo, num_registros):
    chaves = ['chave_desbalanceada'] * int(num_registros * 0.9)
    chaves += ['chave_' + str(i) for i in range(int(num_registros * 0.1))]
    random.shuffle(chaves)

    with open(caminho_arquivo, 'w', newline='') as csvfile:
        escritor = csv.writer(csvfile)
        escritor.writerow(['chave', 'valor'])
        for chave in chaves:
            valor = random.randint(1, 100)
            escritor.writerow([chave, valor])

if __name__ == "__main__":
    gerar_dados('./data/dados.csv', 20000000)
