from sklearn.feature_extraction.text import TfidfVectorizer

def main():
    vectorizer = TfidfVectorizer()
    corpus = [
        "アイデア 機能 ブラウザ スマホ 開発 検索",
        "ドア 機能 システム アイデア",
        "掃除 炊飯器 レシピ アイデア アプリ",
    ]

    scores_of_docs = vectorizer.fit_transform(corpus)
    print(scores_of_docs.toarray())

    word_scores = []
    word_lists = []
    for sentence in corpus:
        word_list = sentence.split(" ")
        word_lists.append(word_list)

    doc_to_score = []
    for idx, scores_of_doc in enumerate(scores_of_docs.toarray()):
        word_to_score = {}    
        for word, score in zip(vectorizer.get_feature_names_out(), scores_of_doc):
            word_to_score[word] = score
        doc_to_score.append(word_to_score)       
            
    for idx, word_list in enumerate(word_lists):
        for word in word_list:
            print(f"{word}: {doc_to_score[idx][word]}")
        print("---------")


if __name__ == "__main__":
    main()