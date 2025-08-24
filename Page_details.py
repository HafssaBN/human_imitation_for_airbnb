from playwright.sync_api import sync_playwright, Page, Frame, FrameLocator, BrowserContext, FilePayload
from undetected_playwright import Tarnished
import json
from selectolax.parser import HTMLParser


def main():
    # with sync_playwright() as p:
    #     p.selectors.set_test_id_attribute('aria-label')
    #     browser = p.chromium.launch(headless=False)
    #     context = browser.new_context()
    #     Tarnished.apply_stealth(context)
    #
    #     page = context.new_page()
    #     page.goto('https://www.airbnb.fr/rooms/36836062')
    #     page.wait_for_load_state('networkidle')
    #     page.wait_for_timeout(2000)
    #
    #     input('Continue... ')
    #     content = page.locator('[id^=data-deferred-state]').all()[0].text_content()
    with open("test.html", "r") as f:
        html = f.read()
    # with open('test.json', 'r') as f:
    #     data = json.load(f)
    content = HTMLParser(html)
    js_section = content.css_first('#data-injector-instances')

    data = json.loads(js_section.text())['root > core-guest-spa'][1][1]
    print(type(js_section.text().encode('utf-8')))
    print(len(js_section.text().encode('utf-8')))
    data = data['niobeMinimalClientData'][1][1]['data']['presentation']['stayProductDetailPage']['sections']
    sbuiData = data['sbuiData']['sectionConfiguration']['root']['sections']
    for section in sbuiData:
        section_id = section['sectionId']
        if section_id == 'GUEST_FAVORITE_BANNER':
            reviewData = section['sectionData']['reviewData']
            reviewsCount = reviewData['reviewsCount']
            averageRating = reviewData['averageRating']
            print(f'Review count: {reviewsCount}')
            print(f'Average rating: {averageRating}')
        if section_id == 'HOST_OVERVIEW_DEFAULT':
            section_data = section['sectionData']
            host = section_data['title'].replace('Hosted by ', '')
            print(f'Host: {host}')
        if section_id == 'LUXE_BANNER':
            print('Airbnb Luxe')
    metadata = data['metadata']
    pdpType = metadata['pdpType']
    print(f'pdpType: {pdpType}')
    pdpUrlType = metadata['pdpUrlType']
    print(f"pdpUrlType: {pdpUrlType}")
    # New data
    sections = data['sections']
    for section in sections:
        sectionId = section['sectionId']
        section_data = section['section']
        if sectionId == 'AVAILABILITY_CALENDAR_DEFAULT':
            location = section_data['localizedLocation']
            print(f'Location: {location}')
            maxGuestCapacity = section_data['maxGuestCapacity']
            print(f'maxGuestCapacity: {maxGuestCapacity}')
        if sectionId == 'REVIEWS_DEFAULT':
            overallCount = section_data['overallCount']
            print(f'overallCount: {overallCount}')
            overallRating = section_data['overallRating']
            print(f'overallRating: {overallRating}')
            isGuestFavorite = section_data['isGuestFavorite']
            print(f'isGuestFavorite: {isGuestFavorite}')
        if sectionId == 'LOCATION_DEFAULT':
            lat = section_data['lat']
            print(f'lat: {lat}')
            lng = section_data['lng']
            print(f'lng: {lng}')
        if sectionId == 'MEET_YOUR_HOST':
            cardData = section_data['cardData']
            name = cardData['name']
            print(f'Name: {name}')
            isSuperhost = cardData['isSuperhost']
            print(f'isSuperhost: {isSuperhost}')
            isVerified = cardData['isVerified']
            print(f'isVerified: {isVerified}')
            hostRatingCount = cardData['ratingCount']
            print(f'ratingCount: {hostRatingCount}')
            userId = cardData['userId']
            print(f'userId: {userId}')
            timeAsHost = cardData['timeAsHost']
            years = timeAsHost['years']
            print(f'years: {years}')
            months = timeAsHost['months']
            print(f'Months: {months}')
            hostrAtingAverage = cardData['ratingAverage']
            print(f'host ratingAverage: {hostrAtingAverage}')
        if sectionId == 'TITLE_DEFAULT':
            title = section_data['title']
            print(f'Title: {title}')
            picture = section_data['shareSave']['embedData']['pictureUrl']
            print(f'picture: {picture}')
        if sectionId == 'AMENITIES_DEFAULT':
            items = section_data['seeAllAmenitiesGroups'][:-1]
            for item in items:
                title = item['title']
                amenities = item['amenities']
                print(title)
                for amenity in amenities:
                    print(f'\t> {amenity["title"]}')

if __name__ == '__main__':
    main()