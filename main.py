import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes

# Вставьте сюда токен от BotFather
TOKEN = "8486101545:AAFz_Me0a-laCQFWFQk1_BZKS0xJOxTzljw"

# Включаем логирование
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)

# Команда /start
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Создаем inline-кнопки (встроенные в сообщение)
    keyboard = [
        [InlineKeyboardButton("📚 Обзор материалов канала", url="https://kadinfo.ru/2025/12/25/itogi/")],
        [InlineKeyboardButton("🎓 Главный курс", callback_data="main_course")],
        [InlineKeyboardButton("📅 Консультация (Calendly)", url="https://calendly.com/ваша-ссылка")],
        [InlineKeyboardButton("📖 Топ-5 статей в блоге", callback_data="top_articles")],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    welcome_text = (
        "Добро пожаловать! Я — навигатор по материалам канала *Практика землепользования*.\n"
        "Выберите, что вас интересует:"
    )
    await update.message.reply_text(welcome_text, reply_markup=reply_markup, parse_mode='Markdown')

# Обработка нажатий на inline-кнопки
async def button_click(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()  # Подтверждаем нажатие

    # В зависимости от callback_data кнопки — разный ответ
    if query.data == "main_course":
        text = "Подробнее о моём основном курсе «Название курса»:\n\n• Что входит: ...\n• Стоимость: ...\n• Отзывы: ...\n\n[👉 Перейти к описанию и покупке](https://ваш-сайт.ru/course)"
        await query.edit_message_text(text=text, parse_mode='Markdown', disable_web_page_preview=False)
    elif query.data == "top_articles":
        text = ("**Самые полезные статьи:**\n\n"
                "1. [Как сделать XYZ](https://ваш-сайт.ru/article1)\n"
                "2. [5 ошибок в ABC](https://ваш-сайт.ru/article2)\n"
                "3. [Полное руководство по DEF](https://ваш-сайт.ru/article3)")
        await query.edit_message_text(text=text, parse_mode='Markdown', disable_web_page_preview=True)

# Главная функция
def main():
    # Создаем приложение
    application = Application.builder().token(TOKEN).build()

    # Добавляем обработчики
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CallbackQueryHandler(button_click))

    # Запускаем бота
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == '__main__':
    main()