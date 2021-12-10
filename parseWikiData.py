
import sys
from gensim.corpora import WikiCorpus

def preprocessdata(input_file, output_file):

	
	print("Processing Started.....!")
	output = open(output_file, 'w')
	wiki = WikiCorpus(input_file)

	
	for text in wiki.get_texts():
		output.write(bytes(' '.join(text), 'utf-8').decode('utf-8') + '\n')
		
		
	output.close()
	print('Processing complete!')


if __name__ == '__main__':

	input_file = sys.argv[1]
	output_file = sys.argv[2]
	preprocessdata(input_file, output_file)

