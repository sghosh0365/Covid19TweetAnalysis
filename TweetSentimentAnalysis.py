from textblob import TextBlob
import re


def getSubjectivity(text):
    return TextBlob(text).sentiment.subjectivity


def getPolarity(text):
    return TextBlob(text).sentiment.polarity


def getAnalysis(score):
    if score < 0:
        return 'Negative'
    elif score == 0:
        return 'Neutral'
    else:
        return 'Positive'


def TweetSentiment(text):
    text = re.sub(r'@[A-Za-z0-9]+', '', text)  # Removes @mentions
    text = re.sub(r'#', '', text)  # Removes the # symbol
    text = re.sub(r'RT[\S]+', '', text)  # Removes RT
    text = re.sub(r'https?:\/\/\S+', '', text)  # Removes the hyper link
    subjectivity = getSubjectivity(text)
    polarity = getPolarity(text)
    score = getAnalysis(polarity)
    return polarity, subjectivity, score
