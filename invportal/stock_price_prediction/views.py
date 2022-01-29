from django.shortcuts import render


def index(request):
    context = {
        'title': 'Stocks price prediction'
    }
    return render(request, "stock_price_prediction/index.html", context=context)
