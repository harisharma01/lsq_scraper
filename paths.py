class ElementsPaths:
        
    def __init__(self):

        self.fill_email_id_xp = '//*[@id="EmailID"]' 
        self.fill_password_xp = '//*[@id="Password"]'
        # self.click_login_button_xp = '//*[@id="kt_login_signin_submit"]'
        self.click_login_button_xp = '//button[@class="btn-primary btn lsq-signin-button"]'
        self.next_button = '//*[@id="login-form-container"]/div[2]/div/div/div/div[3]/button' #same for verify button

        self.full_otp_container = '//*[@id="login-form-container"]/div[2]/div/div/div/div[2]/div[1]/div'
        self.otp_input_xpath = '//*[@class="pincode-input-container"]'

        self.single_input_opt_1 = '//*[@id="login-form-container"]/div[2]/div/div/div/div[2]/div[1]/div/input[1]'
        self.single_input_opt_2 = '//*[@id="login-form-container"]/div[2]/div/div/div/div[2]/div[1]/div/input[2]'
        self.single_input_opt_3 = '//*[@id="login-form-container"]/div[2]/div/div/div/div[2]/div[1]/div/input[3]'
        self.single_input_opt_4 = '//*[@id="login-form-container"]/div[2]/div/div/div/div[2]/div[1]/div/input[4]'
        self.single_input_opt_5 = '//*[@id="login-form-container"]/div[2]/div/div/div/div[2]/div[1]/div/input[5]'
        self.single_input_opt_6 = '//*[@id="login-form-container"]/div[2]/div/div/div/div[2]/div[1]/div/input[6]'

        self.trust_device_checkbox = '//*[@id="divTrustCurrentBrowser"]/label/span'