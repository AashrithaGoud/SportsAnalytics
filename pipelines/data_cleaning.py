def clean_text(text):

    text=str(text).strip()
    text=text.replace('&nbsp', '')
    if text.find(' ♦'):
        text=text.split(' ♦')[0]
    if text.find('['):
        text=text.split('[')[0]

    return text.replace('\n','')




