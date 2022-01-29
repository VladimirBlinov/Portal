from django.shortcuts import render


def index(request):
    context = {
        'title': 'InvPortal Homepage'
    }
    return render(request, "main/index.html", context=context)

