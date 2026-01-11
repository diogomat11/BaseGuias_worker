import os
import time
import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, UnexpectedAlertPresentException

from sqlalchemy.orm import Session
from database import SessionLocal
from models import Log

# Class to handle Scraping
class UnimedScraper:
    def __init__(self, db: Session = None):
        self.driver = None
        self.username = os.environ.get("SGUCARD_LOGIN", "REC2209525")
        self.password = os.environ.get("SGUCARD_PASSWORD", "Unimed@2025")
        self.headless = os.environ.get("SGUCARD_HEADLESS", "false").lower() == "true"
        self.db = db if db else SessionLocal()
        
    def log(self, message, level="INFO", job_id=None, carteirinha_id=None):
        print(f"[{level}] {message}")
        if self.db:
            try:
                log_entry = Log(
                    job_id=job_id,
                    carteirinha_id=carteirinha_id,
                    level=level,
                    message=message
                )
                self.db.add(log_entry)
                self.db.commit()
            except Exception as e:
                print(f"Failed to write log to DB: {e}")

    def funccarteira(self, carteirinha):
        # carteirinha format example: 0064.8000.400948.00-5
        # Remove punctuation for processing if needed, or split by generic delimiters
        # Based on usage: x1, x2, x3, x4, x5 = self.funccarteira(carteirinha)
        # And usage in form filling:
        # cartCompleto = x1 + x2 + x3 + x4 + x5
        # cartaoParcial = x2 + x3 + x4 + x5
        
        # Make it robust to separators
        import re
        parts = re.split(r'[.-]', carteirinha)
        if len(parts) == 5:
            return parts[0], parts[1], parts[2], parts[3], parts[4]
        else:
            # Fallback for raw number string if punctuation missing?
            # Assuming strict format was enforced upstream
            return parts[0], parts[1], parts[2], parts[3], parts[4]

    def start_driver(self):
        chrome_options = Options()
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-infobars")
        chrome_options.add_argument("--disable-gpu")
        if self.headless:
            chrome_options.add_argument("--headless")
        
        self.driver = webdriver.Chrome(options=chrome_options)
        self.driver.maximize_window()

    def close_driver(self):
        if self.driver:
            self.driver.quit()

    def login(self):
        if not self.driver:
            self.start_driver()
            
        try:
            self.driver.get("https://sgucard.unimedgoiania.coop.br/cmagnet/Login.do")
            
            WebDriverWait(self.driver, 20).until(EC.presence_of_element_located((By.ID, "passwordTemp")))
            
            login_elem = self.driver.find_element(By.ID, "login")
            passwordTemp = self.driver.find_element(By.ID, "passwordTemp")
            Button_DoLogin = self.driver.find_element(By.ID, "Button_DoLogin")
            
            login_elem.clear()
            login_elem.send_keys(self.username)
            time.sleep(1)
            passwordTemp.clear()
            passwordTemp.send_keys(self.password)
            Button_DoLogin.click()
            time.sleep(4)
            self.log("Login performed")
        except Exception as e:
            self.log(f"Login failed: {e}", level="ERROR")
            raise e
        
    # (Since I cannot easily insert methods without replacing large chunks, I will replace process_carteirinha fully)

    def process_carteirinha(self, carteirinha, job_id=None, carteirinha_db_id=None):
        # Returns list of guias dicts
        self.log(f"Processing carteirinha: {carteirinha}", job_id=job_id, carteirinha_id=carteirinha_db_id)
        
        handles = self.driver.window_handles
        if len(handles) > 1:
            self.driver.switch_to.window(handles[0])
        
        try:
            # Check if we need to login again or navigate?
            # Assuming we are at the logged in state.
            
            # Helper to check element presence
            def is_element_present(by, value):
                try:
                    self.driver.find_element(by, value)
                    return True
                except NoSuchElementException:
                    return False

            # Sort by Date (click header twice)
            self.log("Sorting table by date (Clicking header twice)...", job_id=job_id, carteirinha_id=carteirinha_db_id)
            try:
                # Based on original script: //*[@id="conteudo-submenu"]/table[2]/tbody/tr[1]/td[1]/a
                header_xpath = '//*[@id="conteudo-submenu"]/table[2]/tbody/tr[1]/td[1]/a'
                if is_element_present(By.XPATH, header_xpath):
                    # First Click
                    self.driver.find_element(By.XPATH, header_xpath).click()
                    self.log("Clicked header once. Waiting 4s...", job_id=job_id, carteirinha_id=carteirinha_db_id)
                    time.sleep(4)
                    
                    # Re-find element to avoid stale reference
                    self.driver.find_element(By.XPATH, header_xpath).click()
                    self.log("Clicked header twice. Waiting 2s...", job_id=job_id, carteirinha_id=carteirinha_db_id)
                    time.sleep(2)
                else:
                    self.log("Sort header not found. Proceeding without explicit sort.", level="WARNING", job_id=job_id, carteirinha_id=carteirinha_db_id)
            except Exception as sort_e:
                self.log(f"Error while sorting table: {sort_e}", level="ERROR", job_id=job_id, carteirinha_id=carteirinha_db_id)

            self.log("Starting scraping loop...", job_id=job_id, carteirinha_id=carteirinha_db_id)
            try:
                # Update XPath or try multiple?
                # User says: "não foi clicado no elemento new_exame"
                WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.XPATH, '//*[@id="cadastro_biometria"]/div/div[2]/span')))
                new_exame = self.driver.find_element(By.XPATH, '//*[@id="cadastro_biometria"]/div/div[2]/span')
                new_exame.click()
                self.log("Clicked 'new_exame'", job_id=job_id, carteirinha_id=carteirinha_db_id)
            except Exception as e:
                self.log(f"Failed to find/click 'new_exame': {str(e)}", level="ERROR", job_id=job_id, carteirinha_id=carteirinha_db_id)
                raise e

            time.sleep(3)
            
            if len(self.driver.window_handles) > 1:
                self.driver.switch_to.window(self.driver.window_handles[-1])
                self.driver.maximize_window()
                self.log("Switched to popup window", job_id=job_id, carteirinha_id=carteirinha_db_id)
            else:
                self.log("Popup window did not open!", level="ERROR", job_id=job_id, carteirinha_id=carteirinha_db_id)
                raise Exception("Popup window not found")
            
            x1, x2, x3, x4, x5 = self.funccarteira(carteirinha)
            cartCompleto = x1 + x2 + x3 + x4 + x5      
            cartaoParcial = x2 + x3 + x4 + x5
            
            self.log("Filling form...", job_id=job_id, carteirinha_id=carteirinha_db_id)
            # Form Filling
            element7 = self.driver.find_element(By.NAME, 'nr_via')
            element6 = self.driver.find_element(By.NAME, 'DS_CARTAO')
            element3 = self.driver.find_element(By.NAME, 'CD_DEPENDENCIA')
            
            self.driver.execute_script("arguments[0].setAttribute('type', 'text');", element7)
            element7.clear()
            element7.send_keys(cartCompleto)
            
            self.driver.execute_script("arguments[0].setAttribute('type', 'text');", element6)
            element6.clear()
            element6.send_keys(cartaoParcial)
            
            self.driver.execute_script("arguments[0].setAttribute('type', 'text');", element3)
            element3.clear()
            element3.send_keys(x3)
            
            if x1 != "0064":
                 self.log(f"Carteirinha prefix {x1} != 0064. Checking Validade...", job_id=job_id, carteirinha_id=carteirinha_db_id)
                 if len(self.driver.find_elements(By.XPATH, '//*[@id="Button_Consulta"]')) > 0:
                      self.driver.find_element(By.XPATH, '//*[@id="Button_Consulta"]').click()
                      time.sleep(2)
            
            # Wait for results table
            self.log("Waiting for Results Table...", job_id=job_id, carteirinha_id=carteirinha_db_id)
            try:
                WebDriverWait(self.driver, 20).until(EC.presence_of_element_located((By.XPATH, '//*[@id="s_NR_GUIA"]')))
            except TimeoutException:
                 self.log("Timeout waiting for results table. Maybe no guias or connection error.", level="WARNING", job_id=job_id, carteirinha_id=carteirinha_db_id)
                 # Close popup and return empty
                 self.driver.close()
                 self.driver.switch_to.window(self.driver.window_handles[0])
                 return []

            collected_data = [] 
            
            # Helper to check element presence
            def is_element_present(by, value):
                try:
                    self.driver.find_element(by, value)
                    return True
                except NoSuchElementException:
                    return False

            # Sort by Date (click header twice)
            self.log("Sorting table by date (Clicking header twice)...", job_id=job_id, carteirinha_id=carteirinha_db_id)
            try:
                # Based on original script: //*[@id="conteudo-submenu"]/table[2]/tbody/tr[1]/td[1]/a
                header_xpath = '//*[@id="conteudo-submenu"]/table[2]/tbody/tr[1]/td[1]/a'
                if is_element_present(By.XPATH, header_xpath):
                    # First Click
                    self.driver.find_element(By.XPATH, header_xpath).click()
                    self.log("Clicked header once. Waiting 4s...", job_id=job_id, carteirinha_id=carteirinha_db_id)
                    time.sleep(4)
                    
                    # Re-find element to avoid stale reference
                    self.driver.find_element(By.XPATH, header_xpath).click()
                    self.log("Clicked header twice. Waiting 2s...", job_id=job_id, carteirinha_id=carteirinha_db_id)
                    time.sleep(2)
                else:
                    self.log("Sort header not found. Proceeding without explicit sort.", level="WARNING", job_id=job_id, carteirinha_id=carteirinha_db_id)
            except Exception as sort_e:
                self.log(f"Error while sorting table: {sort_e}", level="ERROR", job_id=job_id, carteirinha_id=carteirinha_db_id)

            self.log("Starting scraping loop...", job_id=job_id, carteirinha_id=carteirinha_db_id)
            
            while True:
                try:
                    # Re-find table elements on each iteration/page
                    DataTable = self.driver.find_element(By.XPATH, '//*[@id="conteudo-submenu"]/table[2]')
                    linhas = DataTable.find_elements(By.TAG_NAME, "tr")
                    # Skip header and maybe footer? Original skipped [1:-1]
                    # Original loop: range(1, x_count - 1) => skipping first (header) and last?
                    # Let's inspect rows to be safe. Usually tr[0] is header.
                    
                    rows_to_process = len(linhas)
                    self.log(f"Found {rows_to_process} rows on page.", job_id=job_id, carteirinha_id=carteirinha_db_id)
                    
                    # Iterate rows
                    # Note: accessing by index is fragile if DOM changes, but following original logic
                    for idx in range(1, rows_to_process - 1):
                        try:
                            # Re-find element to avoid stale reference
                            row_xpath = f'//*[@id="conteudo-submenu"]/table[2]/tbody/tr[{idx+1}]'
                            status_span = self.driver.find_element(By.XPATH, f'{row_xpath}/td[6]/span')
                            
                            # Scroll into view
                            self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", status_span)
                            
                            if status_span.text == "Autorizado":
                                date_element = self.driver.find_element(By.XPATH, f'{row_xpath}/td[1]')
                                date_text = date_element.text.strip()
                                
                                try:
                                    guia_date = datetime.datetime.strptime(date_text, "%d/%m/%Y").date()
                                except:
                                    guia_date = datetime.datetime.now().date()
                                
                                # Date Filter (Old guides)
                                cutoff_date = datetime.datetime.now().date() - datetime.timedelta(days=270) # Using 270 as in original
                                if guia_date < cutoff_date:
                                    self.log(f"Guia date {date_text} is older than limit. Stopping.", job_id=job_id, carteirinha_id=carteirinha_db_id)
                                    # Close popup and return what we have
                                    self.driver.close()
                                    self.driver.switch_to.window(self.driver.window_handles[0])
                                    return collected_data

                                # Click to details
                                link_element = self.driver.find_element(By.XPATH, f'{row_xpath}/td[4]/a')
                                link_element.click()
                                time.sleep(2)
                                
                                # Extract Details
                                try:
                                    # Wait for detail view
                                    if is_element_present(By.XPATH, '//*[@id="Button_Voltar"]'):
                                        # Scrape details
                                        # Using XPaths from original
                                        new_num_guia = self.driver.find_element(By.XPATH, '//*[@id="conteudo-submenu"]/form/table/tbody/tr[3]/td[2]').text
                                        data_auth = self.driver.find_element(By.XPATH, '//*[@id="conteudo-submenu"]/form/table/tbody/tr[4]/td[4]').text
                                        senha = self.driver.find_element(By.XPATH, '//*[@id="conteudo-submenu"]/form/table/tbody/tr[5]/td[2]').text
                                        data_valid = self.driver.find_element(By.XPATH, '//*[@id="CampoValidadeSenha"]').text
                                        cod_terapia = self.driver.find_element(By.XPATH, '/html/body/div[1]/div[13]/div/table/tbody/tr[2]/td[3]/input').get_attribute("value")
                                        qtde_solic = self.driver.find_element(By.XPATH, '/html/body/div[1]/div[13]/div/table/tbody/tr[2]/td[5]').text.strip()
                                        qtde_aut = self.driver.find_element(By.XPATH, '/html/body/div[1]/div[13]/div/table/tbody/tr[2]/td[6]').text.strip()
                                        
                                        guia_data = {
                                            "numero_guia": new_num_guia,
                                            "data_autorizacao": data_auth,
                                            "senha": senha,
                                            "validade_senha": data_valid,
                                            "codigo_terapia": cod_terapia,
                                            "qtde_solicitada": qtde_solic,
                                            "qtde_autorizada": qtde_aut,
                                            "status": "Autorizado"
                                        }
                                        collected_data.append(guia_data)
                                        self.log(f"Scraped Guia {new_num_guia}", job_id=job_id, carteirinha_id=carteirinha_db_id)
                                        
                                        # Go Back
                                        self.driver.find_element(By.XPATH, '//*[@id="Button_Voltar"]').click()
                                        time.sleep(1)
                                    else:
                                         self.log("Detail view not loaded correctly.", level="WARNING", job_id=job_id, carteirinha_id=carteirinha_db_id)
                                         self.driver.back() # Try browser back? or just loop
                                except Exception as inner_e:
                                    self.log(f"Error extracting details: {inner_e}", level="ERROR", job_id=job_id, carteirinha_id=carteirinha_db_id)
                                    # Try to recover navigation
                                    try:
                                        self.driver.execute_script("window.history.go(-1)")
                                    except: pass

                        except Exception as row_e:
                            self.log(f"Error processing row {idx}: {row_e}", level="ERROR", job_id=job_id, carteirinha_id=carteirinha_db_id)
                            continue

                    # Pagination
                    try:
                         next_link = self.driver.find_element(By.LINK_TEXT, "Próxima")
                         self.log("Navigating to next page...", job_id=job_id, carteirinha_id=carteirinha_db_id)
                         next_link.click()
                         time.sleep(2)
                    except NoSuchElementException:
                        self.log("No more pages.", job_id=job_id, carteirinha_id=carteirinha_db_id)
                        break

                except Exception as table_e:
                    self.log(f"Error validating table loop: {table_e}", level="ERROR", job_id=job_id, carteirinha_id=carteirinha_db_id)
                    break
            
            self.driver.close()
            self.driver.switch_to.window(self.driver.window_handles[0])
            
            return collected_data 

        except Exception as e:
            self.log(f"Error processing carteirinha: {e}", level="ERROR", job_id=job_id, carteirinha_id=carteirinha_db_id)
            if len(self.driver.window_handles) > 1:
                self.driver.close()
                self.driver.switch_to.window(self.driver.window_handles[0])
            raise e

# Main execution if run directly
if __name__ == "__main__":
    s = UnimedScraper()
    s.login()
    # s.process_carteirinha("...")