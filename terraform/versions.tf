terraform {
  required_providers {
    # Провайдер Яндекс Cloud — управляет ресурсами YC через Terraform
    yandex = {
      source  = "yandex-cloud/yandex"
      version = ">= 0.13"
    }
  }
  # Минимальная версия Terraform
  required_version = ">= 1.0.0"
}

# Настройка провайдера: авторизация через переменную окружения YC_TOKEN
# (токен задаётся в терминале: export YC_TOKEN=$(yc iam create-token))
provider "yandex" {
  # token берётся из YC_TOKEN или yc CLI — не хардкодим в файле
}
