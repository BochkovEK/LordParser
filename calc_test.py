MAX_VOTERS = 100
rating_kp_imdb = 8.5
rating_lordfilm = 5.1
number_of_voters = 49

a, b, c = 8.5, 5.1, 49
w = (c - 1) / MAX_VOTERS
result = a * (1 - w) + b * w
print(round(result, 1))  # Вывод: 8.0