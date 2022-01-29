from django.shortcuts import render


def index(request):
    context = {
        'title': 'Stocks price patterns'
    }
    return render(request, "stock_price_patterns/index.html", context=context)
