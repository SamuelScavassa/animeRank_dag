class ExtractData:
    @staticmethod
    def extracao(url):
        import requests
        from bs4 import BeautifulSoup

        map_list = []
        with open('map.txt', 'r') as arquivo:
            map_list = [linha.strip() for linha in arquivo]

        response = requests.get(url)

        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            filas = soup.find_all('tr', class_='ranking-list')

            for fila in filas:
                result_dict = {}
                clasificacion = fila.find('span', class_='lightLink').text.strip()
                result_dict["clasificacion"] = clasificacion
                titulo_anime = fila.find('h3', class_='anime_ranking_h3').text.strip()
                result_dict["titulo_anime"] = titulo_anime
                info_anime = fila.find('div', class_='information').text.strip()
                result_dict["info_anime"] = info_anime
                puntuacion_anime = fila.find('span', class_='score-label').text.strip()
                result_dict["puntuacion_anime"] = puntuacion_anime
                href = fila.find('a', class_='hoverinfo_trigger')['href']
                result_dict["href"] = href
                response = requests.get(href)

                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, 'html.parser')

                    spaceit_pad_elements = soup.find_all('div', class_='spaceit_pad')

                    for element in spaceit_pad_elements:
                        text_parts = element.stripped_strings
                        key = next(text_parts)
                        value = ' '.join(text_parts)
                        value = value.replace("'", '')
                        value = value.replace(",", ".")
                        result_dict[key] = value

                    mapped = {}
                    for value in result_dict:
                        if value in map_list:
                            mapped[value] = result_dict[value]
                    return mapped

                else:
                    print(f'Error. Code: {response.status_code}')

