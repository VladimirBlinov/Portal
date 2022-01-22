from django.shortcuts import render
from mortgage.forms import MortgageBaseForm

menu = [{'title': 'Mortgage calculator', 'url_name': 'mortgage'},
        {'title': 'Stocks price prediction', 'url_name': 'stock_price_prediction'},
        {'title': 'Stocks price patterns', 'url_name': 'stock_price_patterns'}
        ]


def index(request):
    if request.method == 'POST':
        form = MortgageBaseForm(request.POST)
        print(form.data)
        if form.is_valid():
            print(form.cleaned_data)
    else:
        form = MortgageBaseForm()

    context = {
        'menu': menu,
        'title': 'Mortgage calculator',
        'form': form
    }
    return render(request, "mortgage/index.html",  context=context)
