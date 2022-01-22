from django import forms
from django.utils.safestring import mark_safe


class MortgageBaseForm(forms.Form):
    price_mn = forms.FloatField(min_value=0.1,
                                label='House price',
                                required=True,
                                help_text=mark_safe('Price in millions of RUB <br /> Example: 20 (20 mn RUB)'),
                                widget=forms.TextInput(attrs={'class': 'form-control',
                                                              'placeholder': 'mn RUB'}))
    initial_payment_mn = forms.FloatField(min_value=0.1,
                                          label='Initial payment',
                                          required=True,
                                          widget=forms.TextInput(attrs={'class': 'form-control',
                                                                        'placeholder': 'mn RUB'}))
    period = forms.FloatField(min_value=1,
                              label='Mortgage period',
                              required=True,
                              widget=forms.TextInput(attrs={'class': 'form-control',
                                                            'placeholder': 'years'}))
    loan_rate_pct = forms.FloatField(min_value=0.01,
                                     label='Loan rate',
                                     required=True,
                                     widget=forms.TextInput(attrs={'class': 'form-control',
                                                                   'placeholder': '%'}))

    early_payment = forms.BooleanField(label='Are you going to make early payments periodically?',
                                       required=False,
                                       widget=forms.CheckboxInput(attrs={'class': 'form-check-input'}))
    first_month = forms.FloatField(min_value=1,
                                   label='First month',
                                   required=False,
                                   widget=forms.TextInput(attrs={'class': 'form-control'
                                                                 }))
    frequency = forms.FloatField(min_value=1,
                                 label='Frequency',
                                 required=False,
                                 widget=forms.TextInput(attrs={'class': 'form-control',
                                                               'placeholder': ''}))
    early_pay_amount = forms.FloatField(min_value=1,
                                        label='Early payment amount',
                                        required=False,
                                        widget=forms.TextInput(attrs={'class': 'form-control',
                                                                      'placeholder': ''}))

    def clean_first_month(self):
        cleaned_data = super().clean()
        print(cleaned_data)
        early_payment = cleaned_data.get('early_payment')
        first_month = cleaned_data.get('first_month')
        if early_payment:
            if first_month is None:
                print(first_month)
                msg = forms.ValidationError("This field is required as Early payment option is checked", code='invalid')
                self.add_error('first_month', msg)

        return cleaned_data
