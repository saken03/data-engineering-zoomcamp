import argparse

def count_word_occurrences(text, word):
    words = text.split()
    return words.count(word)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Count the number of word occurrences in a given text.")
    parser.add_argument("text", type=str, help="The input text to search for word occurrences.")
    parser.add_argument("word", type=str, help="The word to count occurrences for.")
    args = parser.parse_args()

    text = args.text
    word = args.word

    occurrences = count_word_occurrences(text, word)
    print(f"The word '{word}' occurs {occurrences} times in the text.")
