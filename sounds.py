import pygame



class sounds:
    
    def __init__(self):       
        pygame.init()
        
    def play(self,sound):
        if sound == 'order':
            my_sound = pygame.mixer.Sound(r'order.wav')   
        elif sound == 'cancel':
            my_sound = pygame.mixer.Sound(r'cancel_order.wav')
        my_sound.play()
    
