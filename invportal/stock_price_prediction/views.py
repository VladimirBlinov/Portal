from django.shortcuts import render

menu = [{'title': 'Mortgage calculator', 'url_name': 'mortgage'},
        {'title': 'Stocks price prediction', 'url_name': 'stock_price_prediction'},
        {'title': 'Stocks price patterns', 'url_name': 'stock_price_patterns'}
        ]


def index(request):
    context = {
        'menu': menu,
        'title': 'Stocks price prediction'
    }
    return render(request, "stock_price_prediction/index.html", context=context)
