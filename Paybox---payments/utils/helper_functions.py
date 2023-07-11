from email.policy import EmailPolicy
import smtplib
import email.utils
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders

def send_email(email_address, email_password):
    error_message = str(error)
    # Enregistrement de l'erreur dans un fichier
    logging.basicConfig(filename='error.log', level=logging.ERROR)
    logger = logging.getLogger()
    logger.error(f"Erreur lors de l'établissement de la connexion SFTP : {error_message}")
    logging.shutdown()

    # Envoi d'un e-mail pour notifier l'erreur
    try:
        send_to_email = ['manonpenicaud@homair.com']
        subject = '[FAIL] Paybox payments - ' + error_message

        msg = MIMEText("L'erreur suivante est survenue pendant l'exécution du script :\n\n" + error_message)

        msg['From'] = email_address
        msg['To'] = ', '.join(send_to_email)
        msg['Subject'] = subject

        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(email_address, email_password)
        text = msg.as_string()
        server.sendmail(email_address, send_to_email, text)
        server.quit()

    except Exception as error_sending:
        error_sending_message = str(error_sending)
        logging.basicConfig(filename='error.log', level=logging.ERROR)
        logging.error(error_sending_message)
        logging.shutdown()