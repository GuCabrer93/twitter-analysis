# -*-coding:Latin-1 -*

from emoji import get_emoji_regexp
from re import sub

# Defining function to remove emojis
def remove_emoji(text):
    return get_emoji_regexp().sub(u'', text)

def clean_data(input):
    # Removing emojis
    output = input.map( lambda my_text: remove_emoji(my_text) )

    # Encoding into ascii (comment out this line if using other languages than English)
    #output = output.map( lambda my_text: my_text.encode("ascii", errors="ignore").decode() )

    # Removing strings starting by $, #, @ or http
    output = output.map( lambda my_text: sub(pattern=r'http(\S+)(\s+)' ,repl=" " ,string=my_text) )
    output = output.map( lambda my_text: sub(pattern=r'http(\S+)$'     ,repl=""  ,string=my_text) )

    output = output.map( lambda my_text: sub(pattern=r'\@(\S+)(\s+)'   ,repl=" " ,string=my_text) )
    output = output.map( lambda my_text: sub(pattern=r'\@(\S+)$'       ,repl=""  ,string=my_text) )

    output = output.map( lambda my_text: sub(pattern=r'\#(\S+)(\s+)'   ,repl=" " ,string=my_text) )
    output = output.map( lambda my_text: sub(pattern=r'\#(\S+)$'       ,repl=""  ,string=my_text) )

    output = output.map( lambda my_text: sub(pattern=r'\$(\S+)(\s+)'   ,repl=" " ,string=my_text) )
    output = output.map( lambda my_text: sub(pattern=r'\$(\S+)$'       ,repl=""  ,string=my_text) )

    # Removing retweets
    output = output.map( lambda my_text: sub(pattern=r'^RT',repl="",string=my_text) )

    # Removing space-like symbols
    output = output.map( lambda my_text: my_text
        .replace( "("  ,' ')
        .replace( ")"  ,' ')
        .replace( "["  ,' ')
        .replace( "]"  ,' ')
        .replace( "{"  ,' ')
        .replace( "}"  ,' ')
        .replace( "\\" ,' ')
        .replace( "/" ,' ')
        .replace( "#"  ," ")
        .replace( "@"  ," ")
        .replace( "$"  ," ")
        .replace( "?"  ," ")
        .replace( "!"  ," ")
        .replace( ":"  ,' ')
        .replace( ";"  ,' ')
        .replace( "."  ,' ')
        .replace( ","  ," ")
        .replace( '"'  ,' ')
        .replace( "'"  ,' ')
    )

    # Removing undesired spaces
    output = output.map( lambda my_text: sub(pattern=r'\s+', repl=" ", string=my_text).strip() )

    # Converting to lowercase
    output = output.map( lambda my_text: my_text.lower() )

    # Removing undesired characters (i.e. all non-alphabetic characters)
    #output = output.map( lambda my_text: sub(pattern=r'[^a-z]',repl="",string=my_text) )

    # Uncomment this line to print first 20 results
    #result.map( lambda my_text: "gcg "+my_text+" gcg" ).pprint(20)

    return output

