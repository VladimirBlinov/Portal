from django.urls import path, include

from mortgage import views


urlpatterns = [
    path('', views.index),
]
