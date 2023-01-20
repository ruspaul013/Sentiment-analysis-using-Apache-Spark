# Importing Tweepy and time
import tweepy
import time

import socket
import json

api_key=''
api_secret=''
access_token =''
access_token_secret=''
bearer_token = r''

search_terms = ["python", "programming", "coding"]

# Bot searches for tweets containing certain keywords
class MyStream(tweepy.StreamingClient):

    # This function gets called when the stream is working
    def on_connect(self):
        print("Connected")

    def __init__(self, csocket):
        super().__init__(bearer_token)
        self.client_socket = csocket

    # This function gets called when a tweet passes the stream
    def on_data(self, data):
        try:  
            msg = json.loads( data )
            msg=msg['data']
            print("new message")
            # if tweet is longer than 140 characters
            if "extended_tweet" in msg:
                # add at the end of each tweet "t_end" 
                self.client_socket.send(str(msg['extended_tweet']['full_text']+"t_end").encode('utf-8'))         
                print(msg['extended_tweet']['full_text'])
            else:
                # add at the end of each tweet "t_end" 
                self.client_socket.send(str(msg['text']+"t_end").encode('utf-8'))
                print(msg['text'])
            return True
        except BaseException as e:
            print("Error on_data: %s" % str(e))
        return True

    def on_error(self, status):
        print(status)
        return True


def sendData(c_socket, keyword):
    print('start sending data from Twitter to socket')
    # authentication based on the credentials
    stream = MyStream(c_socket)
    for term in search_terms:
        stream.add_rules(tweepy.StreamRule(term))
    # Starting stream
    stream.filter(tweet_fields=["referenced_tweets"])


if __name__ == "__main__":
    # server (local machine) creates listening socket
    s = socket.socket()
    host = "0.0.0.0"    
    port = 5555
    s.bind((host, port))
    print('socket is ready')
    # server (local machine) listens for connections
    s.listen(4)
    print('socket is listening')
    # return the socket and the address on the other side of the connection (client side)
    c_socket, addr = s.accept()
    print("Received request from: " + str(addr))
    # select here the keyword for the tweet data
    sendData(c_socket, keyword = search_terms)
