from nltk.tokenize import TweetTokenizer
from nltk.tag.stanford import StanfordPOSTagger, StanfordNERTagger
from nltk.stem import WordNetLemmatizer
import pandas as pd

if __name__ == "__main__":
    pd.set_option('display.max_colwidth', -1)
    
    # Initializations
    data = pd.read_csv("tweets/tweet.csv", sep=';', index_col=False, header=None)
    data.columns = ['tweet_id', 'timestamp', 'lat','lon', 'tweet']
    data['timestamp'] = data['timestamp'].apply(pd.to_datetime)
    data[['lat','lon']] = data[['lat','lon']].apply(pd.to_numeric)
    data = data.sample(n=30) # Small sample for testing
    
    # Tokenizing
    tokenizer = TweetTokenizer(strip_handles=True, reduce_len=True)
    data['tweet_tokenized'] = data['tweet'].apply(tokenizer.tokenize)
    
    # Stanford taggers seems don't work well with Pandas dataframes (stuck a long time and no results shown).
    # Seems related to dataset size too (had to reduce sample data above) 
    texts = data['tweet_tokenized'].tolist()
    
    # Part-of-Speech Tagging with Twitter-optimized model built for Stanford POS Tagger
    postagger = StanfordPOSTagger('util/gate-EN-twitter.model','util/stanford-postagger.jar', encoding='utf8', verbose=True, java_options='-mx2048m')
    texts_tagged = [postagger.tag(text) for text in texts]
    data['tweet_tagged'] = texts_tagged
    
    # Extracting hashtags
    def extract_hashtags(col):
        return [t for t in col if t[1].startswith('HT')]
    data['hashtags'] = data['tweet_tagged'].apply(extract_hashtags)
    
    # Extracting names/verbs/adjectives and lemmatizing (tags: http://www.ling.upenn.edu/courses/Fall_2003/ling001/penn_treebank_pos.html)
    wnl = WordNetLemmatizer()
    def extract_terms(col):
        return [(wnl.lemmatize(t[0]),t[1]) for t in col if t[1].startswith('NN') or t[1].startswith('VB') or t[1].startswith('JJ')]
    data['terms'] = data['tweet_tagged'].apply(extract_terms)
    
    # Named Entities Tagging with 4-class model (Location, Person, Organization, "Misc")
    nertagger = StanfordNERTagger('util/english.conll.4class.distsim.crf.ser.gz','util/stanford-ner.jar', encoding='utf8', verbose=True, java_options='-mx2048m')
    texts_named = [nertagger.tag(text) for text in texts]
    data['tweet_named'] = texts_named
    
    # Extracting entities
    def extract_entities(col):
        return [t for t in col if t[1] != "O"]
    data['entities'] = data['tweet_named'].apply(extract_entities)
        
    # Filtering (needed? how to filter? idea based on paper below)
    data.dropna(axis=0, how='all', subset=['hashtags','entities'], inplace=True)
    
    # TO-DO: Vectorizing
    
    
    # TO-DO: Clustering
    
    
    # TO-DO: Apply in streaming data (how?)
    
    
    data.drop(['tweet_tokenized','tweet_tagged', 'tweet_named'], inplace=True, axis=1)
    
    print(data.head(5))
    