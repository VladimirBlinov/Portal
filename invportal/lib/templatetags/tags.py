from django import template

register = template.Library()


@register.simple_tag()
def get_menu():
    return [{'title': 'Mortgage calculator', 'url_name': 'mortgage'},
            {'title': 'Stocks price prediction', 'url_name': 'stock_price_prediction'},
            {'title': 'Stocks price patterns', 'url_name': 'stock_price_patterns'}
            ]
