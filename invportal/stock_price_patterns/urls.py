from django.urls import path

from stock_price_patterns import views


urlpatterns = [
    path('', views.index, name='stock_price_patterns'),
]
