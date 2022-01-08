from django.shortcuts import render

menu = ['Mortgage calculator', 'Stocks price prediction', 'Stocks price patterns']


def index(request):
    return render(request, "mortgage/index.html", {'menu': menu, 'title': 'Mortgage calculator'})
