package strava

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

func GetAccessToken(clientID, clientSecret, refreshToken string) (string, error) {
	body := strings.NewReader(
		fmt.Sprintf(
			`client_id=%s&client_secret=%s&grant_type=refresh_token&refresh_token=%s`,
			clientID,
			clientSecret,
			refreshToken))

	req, err := http.NewRequest("POST", "https://www.strava.com/api/v3/oauth/token", body)
	if err != nil {
		return "", fmt.Errorf("failed to build strava access token request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to get strava access token: %w", err)
	}
	defer resp.Body.Close()

	var tokenResponse accessTokenResponse
	err = json.NewDecoder(resp.Body).Decode(&tokenResponse)
	if err != nil {
		return "", fmt.Errorf("access token body unmarshal failed: %w", err)
	}

	return tokenResponse.AccessToken, nil
}

type accessTokenResponse struct {
	AccessToken  string `json:"access_token"`
	ExpiresAt    int64  `json:"expires_at"`
	ExpiresIn    int64  `json:"expires_in"`
	RefreshToken string `json:"refresh_token"`
	TokenType    string `json:"token_type"`
}
