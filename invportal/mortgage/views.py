import json

from django.shortcuts import render
from requests import post

from mortgage.forms import MortgageBaseForm


def index(request):
    if request.method == 'POST':
        form = MortgageBaseForm(request.POST)
        print(form.data)
        if form.is_valid():
            print(form.cleaned_data)
    else:
        form = MortgageBaseForm()

    context = {
        'title': 'Mortgage calculator',
        'form': form
    }
    return render(request, "mortgage/index.html",  context=context)


def calendar(request):
    url = 'http://127.0.0.1:5005'
    data = request.POST
    response = post(url, data=data)
    response_data = json.loads(response.text)
    header = response_data[list(response_data.keys())[0]].keys()
    print(header)
    context = {
        'title': 'Mortgage calculator',
        'data': response_data,
        'header': header
    }
    print(context['data']['1'])
    return render(request, "mortgage/calendar.html", context=context)
