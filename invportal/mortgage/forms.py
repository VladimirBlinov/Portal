from django import forms
from django.utils.safestring import mark_safe


class MortgageBaseForm(forms.Form):
    price_mn = forms.FloatField(min_value=0.1,
                                label='House price',
                                required=True,
                                help_text=mark_safe('Price in millions of RUB<br />Example: 20 (20 mn RUB)'),
                                widget=forms.TextInput(attrs={'class': 'form-control',
                                                              'placeholder': 'mn RUB'}))

    initial_payment_mn = forms.FloatField(min_value=0.1,
                                          label='Initial payment',
                                          required=True,
                                          help_text=mark_safe('Years of mortgage<br />Example: 30 (30 years)'),
                                          widget=forms.TextInput(attrs={'class': 'form-control',
                                                                        'placeholder': 'mn RUB'}))

    period = forms.FloatField(min_value=1,
                              label='Mortgage period',
                              required=True,
                              help_text=mark_safe('Amount in millions of RUB<br />Example: 2.5 (2.5 mn RUB)'),
                              widget=forms.TextInput(attrs={'class': 'form-control',
                                                            'placeholder': 'years'}))

    loan_rate_pct = forms.FloatField(min_value=0.01,
                                     label='Loan rate',
                                     required=True,
                                     help_text=mark_safe('Loan rate in %<br>Example: 7.5 (7.5%)'),
                                     widget=forms.TextInput(attrs={'class': 'form-control',
                                                                   'placeholder': '%'}))

    early_payment = forms.BooleanField(label='Are you going to make early payments periodically?',
                                       required=False,
                                       widget=forms.CheckboxInput(attrs={'class': 'form-check-input'}))

    first_month = forms.FloatField(min_value=1,
                                   label='First month',
                                   required=False,
                                   help_text=mark_safe('I will start regular early payments from this month<br />'
                                                       'Example: 24 (from 24th month)'),
                                   widget=forms.TextInput(attrs={'class': 'form-control'
                                                                 }))
    frequency = forms.FloatField(min_value=1,
                                 label='Frequency',
                                 required=False,
                                 help_text=mark_safe('I will make early payments every ..th month<br />Example: '
                                                     '1 (every month);<br>6 (every half of the year)'),
                                 widget=forms.TextInput(attrs={'class': 'form-control',
                                                               'placeholder': ''}))

    early_pay_amount = forms.FloatField(min_value=1,
                                        label='Early payment amount',
                                        required=False,
                                        help_text=mark_safe('Amount of periodical early payment<br />'
                                                            'Example: 50000 (50000 RUB)'),
                                        widget=forms.TextInput(attrs={'class': 'form-control',
                                                                      'placeholder': ''}))

    def fields_required(self, fields):
        """Used for conditionally marking fields as required."""
        for field in fields:
            if not self.cleaned_data.get(field, ''):
                msg = forms.ValidationError("This field is required as Early payment option is checked")
                self.add_error(field, msg)

    def clean(self):
        cleaned_data = super().clean()
        early_payment = cleaned_data.get('early_payment')
        if early_payment:
            self.fields_required(['first_month', 'frequency', 'early_pay_amount'])
