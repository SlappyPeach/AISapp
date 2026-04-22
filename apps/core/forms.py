from __future__ import annotations

from django import forms
from django.conf import settings
from django.contrib.auth.password_validation import validate_password
from django.utils import timezone

from .models import (
    ConstructionObject,
    DocumentStatus,
    DocumentType,
    Material,
    MaterialNorm,
    ProcurementRequest,
    RoleChoices,
    SMRContract,
    StockReceipt,
    Supplier,
    SupplierDocument,
    SupplyContract,
    User,
    Worker,
)
from .reporting import REPORT_CHOICES
from .services import parse_line_items, parse_ppe_lines


class DateInput(forms.DateInput):
    input_type = "date"


EMPTY_CHOICE_LABEL = "Не выбрано"


class BaseStyledForm:
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        for field in self.fields.values():
            widget = field.widget
            css_class = widget.attrs.get("class", "")
            if isinstance(widget, forms.CheckboxInput):
                widget.attrs["class"] = f"{css_class} checkbox-input".strip()
            else:
                widget.attrs["class"] = f"{css_class} form-input".strip()

            if getattr(field, "empty_label", None) == "---------":
                field.empty_label = EMPTY_CHOICE_LABEL
            if hasattr(field, "choices"):
                choices = list(field.choices)
                if choices and choices[0][1] == "---------":
                    field.choices = [(choices[0][0], EMPTY_CHOICE_LABEL), *choices[1:]]

        items_field = self.fields.get("items")
        if items_field and isinstance(items_field.widget, forms.HiddenInput):
            items_mode = items_field.widget.attrs.get("data-items-mode")
            if items_mode == "ppe-lines":
                items_field.help_text = "Заполните строки в таблице ниже: табельный номер, код материала, количество и срок службы."
            elif items_mode == "material-lines":
                if items_field.required:
                    items_field.help_text = "Заполните строки в таблице ниже. Наименование, единица и цена подставляются по коду материала."
                else:
                    items_field.help_text = (
                        "Заполните строки в таблице ниже. Если оставить поле пустым, позиции будут взяты из связанного документа."
                    )


class DateRangeValidationMixin:
    def clean(self):
        cleaned_data = super().clean()
        date_from = cleaned_data.get("date_from")
        date_to = cleaned_data.get("date_to")
        if date_from and date_to and date_to < date_from:
            self.add_error("date_to", "Дата окончания не может быть раньше даты начала.")
        return cleaned_data


WORKFLOW_ENTRY_STATUS_CHOICES = [
    (DocumentStatus.DRAFT, dict(DocumentStatus.choices)[DocumentStatus.DRAFT]),
    (DocumentStatus.APPROVAL, dict(DocumentStatus.choices)[DocumentStatus.APPROVAL]),
]

SUPPLIER_DOCUMENT_TYPE_FALLBACK = [
    ("Счет", "Счет"),
    ("Счет-фактура", "Счет-фактура"),
    ("Товарная накладная", "Товарная накладная"),
    ("Приходная накладная", "Приходная накладная"),
]


def _upload_document_type_choices() -> list[tuple[str, str]]:
    try:
        rows = list(
            DocumentType.objects.filter(is_active=True, available_for_upload=True)
            .order_by("name")
            .values_list("name", "name")
        )
    except Exception:
        rows = []
    return rows or SUPPLIER_DOCUMENT_TYPE_FALLBACK


class ProcurementRequestCreateForm(BaseStyledForm, forms.Form):
    request_date = forms.DateField(widget=DateInput(), initial=timezone.localdate, label="Дата заявки")
    site_name = forms.CharField(max_length=255, label="Участок")
    contract = forms.ModelChoiceField(queryset=SMRContract.objects.order_by("-contract_date"), required=False, label="Договор СМР")
    supplier = forms.ModelChoiceField(queryset=Supplier.objects.order_by("name"), required=False, label="Поставщик")
    status = forms.ChoiceField(choices=WORKFLOW_ENTRY_STATUS_CHOICES, initial=DocumentStatus.DRAFT, label="Статус")
    notes = forms.CharField(required=False, widget=forms.Textarea(attrs={"rows": 3}), label="Комментарий")
    items = forms.CharField(
        widget=forms.HiddenInput(attrs={"data-items-mode": "material-lines"}),
        label="Позиции",
        help_text="Формат строки: КОД | КОЛИЧЕСТВО | ЦЕНА | ПРИМЕЧАНИЕ",
    )

    def clean_items(self):
        items = self.cleaned_data["items"]
        parse_line_items(items)
        return items


class SupplierDocumentUploadForm(BaseStyledForm, forms.Form):
    supplier = forms.ModelChoiceField(queryset=Supplier.objects.order_by("name"), required=False, label="Поставщик")
    doc_type = forms.ChoiceField(choices=SUPPLIER_DOCUMENT_TYPE_FALLBACK, label="Тип документа")
    doc_number = forms.CharField(max_length=128, required=False, label="Номер")
    doc_date = forms.DateField(widget=DateInput(), initial=timezone.localdate, label="Дата")
    amount = forms.DecimalField(max_digits=14, decimal_places=2, required=False, label="Сумма")
    vat_amount = forms.DecimalField(max_digits=14, decimal_places=2, required=False, label="НДС")
    request = forms.ModelChoiceField(queryset=ProcurementRequest.objects.none(), required=False, label="Заявка")
    supply_contract = forms.ModelChoiceField(queryset=SupplyContract.objects.order_by("-contract_date"), required=False, label="Договор поставки")
    notes = forms.CharField(required=False, widget=forms.Textarea(attrs={"rows": 3}), label="Комментарий")
    attachment = forms.FileField(required=False, label="Файл")

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields["request"].queryset = ProcurementRequest.objects.order_by("-request_date")
        self.fields["doc_type"].choices = _upload_document_type_choices()

    def clean(self):
        cleaned_data = super().clean()
        supplier = cleaned_data.get("supplier")
        related_suppliers = [
            item.supplier
            for item in [cleaned_data.get("request"), cleaned_data.get("supply_contract")]
            if item is not None and getattr(item, "supplier_id", None)
        ]
        if supplier and any(item.pk != supplier.pk for item in related_suppliers):
            raise forms.ValidationError("Поставщик документа должен совпадать с поставщиком в заявке или договоре.")
        return cleaned_data


class PrimaryDocumentCreateForm(BaseStyledForm, forms.Form):
    document_type = forms.ModelChoiceField(queryset=DocumentType.objects.none(), label="Тип документа")
    doc_date = forms.DateField(widget=DateInput(), initial=timezone.localdate, label="Дата")
    supplier = forms.ModelChoiceField(queryset=Supplier.objects.order_by("name"), required=False, label="Поставщик")
    request = forms.ModelChoiceField(queryset=ProcurementRequest.objects.order_by("-request_date"), required=False, label="Заявка")
    supply_contract = forms.ModelChoiceField(queryset=SupplyContract.objects.order_by("-contract_date"), required=False, label="Договор поставки")
    stock_receipt = forms.ModelChoiceField(queryset=StockReceipt.objects.order_by("-receipt_date"), required=False, label="Приход на склад")
    status = forms.ChoiceField(choices=WORKFLOW_ENTRY_STATUS_CHOICES, initial=DocumentStatus.DRAFT, label="Статус")
    amount = forms.DecimalField(max_digits=14, decimal_places=2, required=False, label="Сумма")
    vat_amount = forms.DecimalField(max_digits=14, decimal_places=2, required=False, label="НДС")
    notes = forms.CharField(required=False, widget=forms.Textarea(attrs={"rows": 3}), label="Комментарий")
    items = forms.CharField(
        required=False,
        widget=forms.HiddenInput(attrs={"data-items-mode": "material-lines"}),
        label="Позиции",
        help_text="Если оставить поле пустым, позиции будут взяты из заявки или приходного документа. Формат строки: КОД | КОЛИЧЕСТВО | ЦЕНА | ПРИМЕЧАНИЕ",
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields["document_type"].queryset = DocumentType.objects.filter(is_active=True, available_for_generation=True).order_by("name")

    def clean_items(self):
        items = self.cleaned_data.get("items", "")
        if items:
            parse_line_items(items)
        return items


class StockReceiptCreateForm(BaseStyledForm, forms.Form):
    receipt_date = forms.DateField(widget=DateInput(), initial=timezone.localdate, label="Дата прихода")
    supplier = forms.ModelChoiceField(queryset=Supplier.objects.order_by("name"), label="Поставщик")
    supplier_document = forms.ModelChoiceField(queryset=SupplierDocument.objects.none(), required=False, label="Документ поставщика")
    status = forms.ChoiceField(choices=WORKFLOW_ENTRY_STATUS_CHOICES, initial=DocumentStatus.DRAFT, label="Статус")
    notes = forms.CharField(required=False, widget=forms.Textarea(attrs={"rows": 3}), label="Комментарий")
    items = forms.CharField(
        widget=forms.HiddenInput(attrs={"data-items-mode": "material-lines"}),
        label="Позиции",
        help_text="Формат строки: КОД | КОЛИЧЕСТВО | ЦЕНА | ПРИМЕЧАНИЕ",
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields["supplier_document"].queryset = SupplierDocument.objects.order_by("-doc_date")

    def clean_items(self):
        items = self.cleaned_data["items"]
        parse_line_items(items)
        return items

    def clean(self):
        cleaned_data = super().clean()
        supplier = cleaned_data.get("supplier")
        supplier_document = cleaned_data.get("supplier_document")
        if supplier and supplier_document and supplier_document.supplier_id != supplier.id:
            raise forms.ValidationError("Документ поставщика должен принадлежать выбранному поставщику.")
        return cleaned_data


class StockIssueCreateForm(BaseStyledForm, forms.Form):
    issue_date = forms.DateField(widget=DateInput(), initial=timezone.localdate, label="Дата отпуска")
    site_name = forms.CharField(max_length=255, label="Участок")
    contract = forms.ModelChoiceField(queryset=SMRContract.objects.order_by("-contract_date"), required=False, label="Договор СМР")
    received_by_name = forms.CharField(max_length=255, label="Получатель")
    status = forms.ChoiceField(choices=WORKFLOW_ENTRY_STATUS_CHOICES, initial=DocumentStatus.DRAFT, label="Статус")
    notes = forms.CharField(required=False, widget=forms.Textarea(attrs={"rows": 3}), label="Комментарий")
    items = forms.CharField(
        widget=forms.HiddenInput(attrs={"data-items-mode": "material-lines"}),
        label="Позиции",
        help_text="Формат строки: КОД | КОЛИЧЕСТВО | ЦЕНА | ПРИМЕЧАНИЕ",
    )

    def clean_items(self):
        items = self.cleaned_data["items"]
        parse_line_items(items)
        return items


class WriteOffCreateForm(BaseStyledForm, forms.Form):
    act_date = forms.DateField(widget=DateInput(), initial=timezone.localdate, label="Дата акта")
    contract = forms.ModelChoiceField(queryset=SMRContract.objects.order_by("-contract_date"), label="Договор СМР")
    site_name = forms.CharField(max_length=255, label="Участок")
    work_type = forms.CharField(max_length=255, label="Вид работ")
    work_volume = forms.DecimalField(max_digits=14, decimal_places=3, label="Объем работ")
    volume_unit = forms.CharField(max_length=64, required=False, label="Единица объема")
    status = forms.ChoiceField(choices=WORKFLOW_ENTRY_STATUS_CHOICES, initial=DocumentStatus.DRAFT, label="Статус")
    notes = forms.CharField(required=False, widget=forms.Textarea(attrs={"rows": 3}), label="Комментарий")


class PPEIssuanceCreateForm(BaseStyledForm, forms.Form):
    issue_date = forms.DateField(widget=DateInput(), initial=timezone.localdate, label="Дата выдачи")
    site_name = forms.CharField(max_length=255, label="Участок")
    season = forms.ChoiceField(choices=[("летняя", "летняя"), ("зимняя", "зимняя")], required=False, label="Сезон")
    status = forms.ChoiceField(choices=WORKFLOW_ENTRY_STATUS_CHOICES, initial=DocumentStatus.DRAFT, label="Статус")
    notes = forms.CharField(required=False, widget=forms.Textarea(attrs={"rows": 3}), label="Комментарий")
    items = forms.CharField(
        widget=forms.HiddenInput(attrs={"data-items-mode": "ppe-lines"}),
        label="Позиции",
        help_text="Формат строки: ТАБЕЛЬНЫЙ_НОМЕР | КОД_МАТЕРИАЛА | КОЛИЧЕСТВО | СРОК_СЛУЖБЫ_МЕС",
    )

    def clean_items(self):
        items = self.cleaned_data["items"]
        parse_ppe_lines(items)
        return items


class WorkLogCreateForm(BaseStyledForm, forms.Form):
    site_name = forms.CharField(max_length=255, label="Участок")
    contract = forms.ModelChoiceField(queryset=SMRContract.objects.order_by("-contract_date"), required=False, label="Договор СМР")
    work_type = forms.CharField(max_length=255, label="Вид работ")
    planned_volume = forms.DecimalField(max_digits=14, decimal_places=3, required=False, label="Плановый объем")
    actual_volume = forms.DecimalField(max_digits=14, decimal_places=3, required=False, label="Фактический объем")
    volume_unit = forms.CharField(max_length=64, required=False, label="Единица измерения")
    plan_date = forms.DateField(widget=DateInput(), required=False, label="Плановая дата")
    actual_date = forms.DateField(widget=DateInput(), required=False, label="Фактическая дата")
    status = forms.CharField(max_length=64, initial="Запланировано", label="Статус")
    notes = forms.CharField(required=False, widget=forms.Textarea(attrs={"rows": 3}), label="Комментарий")


class ArchiveFilterForm(DateRangeValidationMixin, BaseStyledForm, forms.Form):
    doc_type = forms.CharField(required=False, label="Тип документа")
    doc_number = forms.CharField(required=False, label="Номер")
    status = forms.ChoiceField(required=False, choices=[("", "Все")] + list(DocumentStatus.choices), label="Статус")
    date_from = forms.DateField(required=False, widget=DateInput(), label="С")
    date_to = forms.DateField(required=False, widget=DateInput(), label="По")
    counterparty = forms.CharField(required=False, label="Поставщик / контрагент")
    object_name = forms.CharField(required=False, label="Объект / договор")
    query = forms.CharField(required=False, label="Поиск")


class ReportFilterForm(DateRangeValidationMixin, BaseStyledForm, forms.Form):
    report = forms.ChoiceField(choices=REPORT_CHOICES, label="Отчет")
    date_from = forms.DateField(required=False, widget=DateInput(), label="С")
    date_to = forms.DateField(required=False, widget=DateInput(), label="По")
    material_code = forms.CharField(required=False, label="Код материала")
    location_name = forms.CharField(required=False, label="Участок / склад")
    object_name = forms.CharField(required=False, label="Объект")
    supplier_name = forms.CharField(required=False, label="Поставщик")
    contract_number = forms.CharField(required=False, label="Номер договора")


class AuditLogFilterForm(DateRangeValidationMixin, BaseStyledForm, forms.Form):
    username = forms.CharField(required=False, label="Пользователь")
    action = forms.CharField(required=False, label="Действие")
    entity_type = forms.CharField(required=False, label="Сущность")
    date_from = forms.DateField(required=False, widget=DateInput(), label="С")
    date_to = forms.DateField(required=False, widget=DateInput(), label="По")
    query = forms.CharField(required=False, label="Поиск")


class MaterialForm(BaseStyledForm, forms.ModelForm):
    class Meta:
        model = Material
        fields = ["code", "name", "unit", "price", "min_stock", "category", "is_ppe"]
        labels = {
            "code": "Код",
            "name": "Наименование",
            "unit": "Ед. изм.",
            "price": "Цена",
            "min_stock": "Мин. остаток",
            "category": "Категория",
            "is_ppe": "СИЗ / спецодежда",
        }


class SupplierForm(BaseStyledForm, forms.ModelForm):
    class Meta:
        model = Supplier
        fields = ["name", "tax_id", "contact_person", "phone", "email", "address", "requisites"]
        labels = {
            "name": "Поставщик",
            "tax_id": "ИНН",
            "contact_person": "Контактное лицо",
            "phone": "Телефон",
            "email": "Эл. почта",
            "address": "Адрес",
            "requisites": "Реквизиты",
        }
        widgets = {
            "address": forms.Textarea(attrs={"rows": 3}),
            "requisites": forms.Textarea(attrs={"rows": 4}),
        }


class ConstructionObjectForm(BaseStyledForm, forms.ModelForm):
    class Meta:
        model = ConstructionObject
        fields = ["name", "address", "customer_name", "customer_requisites", "description", "start_date", "end_date"]
        labels = {
            "name": "Наименование объекта",
            "address": "Адрес",
            "customer_name": "Заказчик",
            "customer_requisites": "Реквизиты заказчика",
            "description": "Описание",
            "start_date": "Дата начала",
            "end_date": "Дата окончания",
        }
        widgets = {
            "customer_requisites": forms.Textarea(attrs={"rows": 4}),
            "description": forms.Textarea(attrs={"rows": 3}),
            "start_date": DateInput(),
            "end_date": DateInput(),
        }


class WorkerForm(BaseStyledForm, forms.ModelForm):
    class Meta:
        model = Worker
        fields = ["full_name", "employee_number", "site_name", "position", "hire_date"]
        labels = {
            "full_name": "ФИО",
            "employee_number": "Табельный номер",
            "site_name": "Участок",
            "position": "Должность",
            "hire_date": "Дата приема",
        }
        widgets = {"hire_date": DateInput()}


class MaterialNormForm(BaseStyledForm, forms.ModelForm):
    class Meta:
        model = MaterialNorm
        fields = ["work_type", "material", "norm_per_unit", "unit", "notes"]
        labels = {
            "work_type": "Вид работ",
            "material": "Материал",
            "norm_per_unit": "Норма на единицу",
            "unit": "Ед. изм.",
            "notes": "Примечание",
        }
        widgets = {"notes": forms.Textarea(attrs={"rows": 3})}


class DocumentTypeForm(BaseStyledForm, forms.ModelForm):
    class Meta:
        model = DocumentType
        fields = ["code", "name", "prefix", "is_active", "available_for_upload", "available_for_generation", "requires_items", "description"]
        labels = {
            "code": "Код",
            "name": "Наименование",
            "prefix": "Префикс",
            "is_active": "Активен",
            "available_for_upload": "Доступен для загрузки",
            "available_for_generation": "Доступен для генерации",
            "requires_items": "Требует позиции",
            "description": "Описание",
        }
        widgets = {"description": forms.Textarea(attrs={"rows": 3})}


class SMRContractForm(BaseStyledForm, forms.ModelForm):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if "customer_name" in self.fields:
            self.fields["customer_name"].required = False

        profile = getattr(settings, "ORGANIZATION_PROFILE", {}) or {}
        if not self.is_bound:
            if "contractor_name" in self.fields and not self.initial.get("contractor_name"):
                self.fields["contractor_name"].initial = str(profile.get("name", "")).strip()
            if "contractor_requisites" in self.fields and not self.initial.get("contractor_requisites"):
                self.fields["contractor_requisites"].initial = str(profile.get("requisites", "")).strip()

    def clean(self):
        cleaned_data = super().clean()
        construction_object = cleaned_data.get("object")

        customer_name = (cleaned_data.get("customer_name") or "").strip()
        if not customer_name and construction_object:
            customer_name = (construction_object.customer_name or "").strip()
            cleaned_data["customer_name"] = customer_name
        if not customer_name:
            self.add_error("customer_name", "Укажите заказчика или выберите объект с заполненными данными заказчика.")

        customer_requisites = (cleaned_data.get("customer_requisites") or "").strip()
        if not customer_requisites and construction_object:
            cleaned_data["customer_requisites"] = (construction_object.customer_requisites or "").strip()

        profile = getattr(settings, "ORGANIZATION_PROFILE", {}) or {}
        contractor_name = (cleaned_data.get("contractor_name") or "").strip()
        if not contractor_name:
            cleaned_data["contractor_name"] = str(profile.get("name", "")).strip()

        contractor_requisites = (cleaned_data.get("contractor_requisites") or "").strip()
        if not contractor_requisites:
            cleaned_data["contractor_requisites"] = str(profile.get("requisites", "")).strip()

        return cleaned_data

    class Meta:
        model = SMRContract
        fields = [
            "number",
            "contract_date",
            "object",
            "customer_name",
            "customer_requisites",
            "contractor_name",
            "contractor_requisites",
            "subject",
            "work_type",
            "planned_volume",
            "volume_unit",
            "amount",
            "vat_rate",
            "start_date",
            "end_date",
            "status",
        ]
        labels = {
            "number": "Номер",
            "contract_date": "Дата договора",
            "object": "Объект строительства",
            "customer_name": "Заказчик",
            "customer_requisites": "Реквизиты заказчика",
            "contractor_name": "Подрядчик",
            "contractor_requisites": "Реквизиты подрядчика",
            "subject": "Предмет договора",
            "work_type": "Вид работ",
            "planned_volume": "Плановый объем",
            "volume_unit": "Ед. изм. объема",
            "amount": "Сумма",
            "vat_rate": "Ставка НДС",
            "start_date": "Дата начала",
            "end_date": "Дата окончания",
            "status": "Статус",
        }
        widgets = {
            "contract_date": DateInput(),
            "customer_requisites": forms.Textarea(attrs={"rows": 4}),
            "contractor_requisites": forms.Textarea(attrs={"rows": 4}),
            "start_date": DateInput(),
            "end_date": DateInput(),
        }


class SupplyContractForm(BaseStyledForm, forms.ModelForm):
    class Meta:
        model = SupplyContract
        fields = ["number", "contract_date", "supplier", "related_smr_contract", "amount", "status", "terms"]
        labels = {
            "number": "Номер",
            "contract_date": "Дата договора",
            "supplier": "Поставщик",
            "related_smr_contract": "Связанный договор СМР",
            "amount": "Сумма",
            "status": "Статус",
            "terms": "Условия",
        }
        widgets = {
            "contract_date": DateInput(),
            "terms": forms.Textarea(attrs={"rows": 3}),
        }


class UserForm(BaseStyledForm, forms.ModelForm):
    role = forms.ChoiceField(choices=RoleChoices.choices, label="Роль")
    password1 = forms.CharField(required=False, widget=forms.PasswordInput(), label="Пароль")
    password2 = forms.CharField(required=False, widget=forms.PasswordInput(), label="Подтверждение пароля")

    class Meta:
        model = User
        fields = ["username", "first_name", "last_name", "email", "role", "site_name", "supplier", "is_active"]
        labels = {
            "username": "Логин",
            "first_name": "Имя",
            "last_name": "Фамилия",
            "email": "Адрес электронной почты",
            "site_name": "Участок / подразделение",
            "supplier": "Поставщик",
            "is_active": "Активный",
        }
        help_texts = {
            "username": "Используйте буквы, цифры и символы @/./+/-/_.",
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields["supplier"].required = False
        if self.instance.pk:
            self.fields["password1"].help_text = "Оставьте пустым, чтобы не менять пароль."
            self.fields["password2"].help_text = "Заполните только если меняете пароль."
        else:
            self.fields["password1"].help_text = "Укажите пароль для новой учетной записи."
            self.fields["password2"].help_text = "Повторите пароль."

    def clean(self):
        cleaned_data = super().clean()
        role = cleaned_data.get("role")
        supplier = cleaned_data.get("supplier")
        password1 = cleaned_data.get("password1") or ""
        password2 = cleaned_data.get("password2") or ""

        if role == RoleChoices.SUPPLIER:
            if supplier is None:
                self.add_error("supplier", "Для роли поставщика нужно выбрать связанного поставщика.")
        else:
            cleaned_data["supplier"] = None

        if not self.instance.pk and not password1:
            self.add_error("password1", "Укажите пароль для новой учетной записи.")

        if password1 or password2:
            if password1 != password2:
                self.add_error("password2", "Пароли должны совпадать.")
            else:
                validate_password(password1, self.instance if self.instance.pk else None)

        return cleaned_data

    def save(self, commit=True):
        user = super().save(commit=False)
        if self.cleaned_data.get("role") != RoleChoices.SUPPLIER:
            user.supplier = None

        password = self.cleaned_data.get("password1")
        if password:
            user.set_password(password)

        if commit:
            user.save()
        return user


class BackupRestoreUploadForm(BaseStyledForm, forms.Form):
    backup_file = forms.FileField(label="JSON-файл резервной копии")
    confirm_restore = forms.BooleanField(label="Подтверждаю замену текущих данных при восстановлении")
